# Final Project:Welfare Loss from Urban Boundaries: China's Farmland Protection Policy

- [Final Project:Welfare Loss from Urban Boundaries: China's Farmland Protection Policy](#final-projectwelfare-loss-from-urban-boundaries-chinas-farmland-protection-policy)
  - [Introduction](#introduction)
  - [Data](#data)
    - [Data Sources](#data-sources)
    - [Data Cleaning and Wrangling](#data-cleaning-and-wrangling)
      - [Preprocessing Rural Rent and Urban Housing Data](#preprocessing-rural-rent-and-urban-housing-data)
      - [Rural Land Price Estimation](#rural-land-price-estimation)
      - [1. **Match with Administrative Units**](#1-match-with-administrative-units)
      - [2. **Exclude Urban Areas**](#2-exclude-urban-areas)
      - [3. **Compute Average Land Price**](#3-compute-average-land-price)
  - [Computation Workflow](#computation-workflow)
    - [**1. Rent Curve Fitting**](#1-rent-curve-fitting)
    - [**2. Computing the Theoretical Urban Boundary**](#2-computing-the-theoretical-urban-boundary)
    - [**3. Measuring the Urban Expansion Gap**](#3-measuring-the-urban-expansion-gap)
    - [**4. Welfare Loss Estimation**](#4-welfare-loss-estimation)
    - [**Supporting Functions**](#supporting-functions)
  - [Embarassingly Parallel Computation and Efficiency Comparison](#embarassingly-parallel-computation-and-efficiency-comparison)
    - [Midway: Slurm Array](#midway-slurm-array)
    - [AWS Batch with EC2 Instances](#aws-batch-with-ec2-instances)
    - [Analysis](#analysis)


## Introduction

This project aims to quantify the welfare loss resulting from China’s farmland protection policy, which restricts urban expansion through the enforcement of a redline boundary for cultivated land. I use satellite-based built-up area data and housing price information to estimate urban land rent gradients, and identify both the observed urban-rural boundary and the theoretical boundary implied by an unconstrained monocentric city model. The welfare loss is measured as the area under the estimated rent curve between the theoretical and actual urban boundaries, reflecting the economic cost of preventing urban expansion into more productive adjacent land.

## Data
### Data Sources

1. **GAEZ v4 Agro-Ecological Data**
   [Gaze-v4](Data/Raw/Gaze_2010/all_2010_val.tif)
   [Website](https://gaez.fao.org/pages/data-access-download)
   [Description] (https://gaez-services.fao.org/server/rest/services/res06/ImageServer)
   Global Agro-Ecological Zones (GAEZ v4) Theme 5 provides spatial layers of harvested area, yield, and production at a 5 arc-minute resolution for 26 major crops, distinguishing between rain-fed and irrigated cropland. The dataset includes estimates of total crop production value, valued at 2000 international prices, based on FAO statistics for 2009–2011. 


2. **Housing Prices and Geographic Coordinates of Residential Communities in China (2022)**
   [Access Data on Google Drive](https://drive.google.com/file/d/1_WlK02m4RbDZn8OsyrtL5e1ypuQjuz1g/view?usp=drive_link)
   A dataset of average 2022 housing prices and geographic locations for residential neighborhoods across Chinese cities.

3. **Built-Up Area Dataset of Chinese Cities (2020)**
   [Sun Zhongchang, Sun Jie, Guo Huadong, et al. *Built-Up Area Dataset of Chinese Cities, 2020* [DS/OL]. V1. Science Data Bank, 2021
   CSTR: 31253.11.sciencedb.j00001.00332.](https://cstr.cn/31253.11.sciencedb.j00001.00332)

4. **Administrative Boundary Shapefiles of China (2020)**
   [Data File](Data/Raw/China_Adm_2020/China2020County.shp)
   Shapefile data for provincial and municipal administrative boundaries in China as of 2020.


### Data Cleaning and Wrangling

#### Preprocessing Rural Rent and Urban Housing Data

[data_clean_loc.ipynb](Code/data_clean_loc.ipynb)
1. **Estimating Rural Land Rent**

    I estimate rural land rent based on agricultural land productivity using the GAEZ v4 dataset. Specifically, 2010 production values are adjusted to 2022 RMB using historical exchange rates and CPI.

    **Steps:**

    * Convert `value` to GK\$ by multiplying by 1,000.
    * Convert GK\$ (≈ USD 2000) to RMB using 1 USD = 8.28 RMB.
    * Adjust to 2022 RMB using cumulative CPI.
    * Calculate pixel area based on latitude for square meter conversion.
    * Derive `price_m2` as the estimated annual output per m² in 2022 RMB.

    The cleaned raster data is clipped to China's territory and saved as `agr_val_2022.csv`.

2. **Matching Residential Communities with Administrative Units**

    I matched residential community coordinates (from 2022 housing price data) to 2020 administrative boundaries. Due to minor geolocation errors, unmatched points were dropped.

    * **Total points:** 899,296
    * **Matched:** 899,161 (99.98%)
    * **Unmatched:** 135

    The matched data is used for all subsequent analysis.

3. **Filtering for Urban Areas**

    I restrict analysis to residential points located within built-up areas using the 2020 built-up area dataset:

    * Merged all provincial shapefiles for cities >300k population.
    * Performed spatial join with the housing price data.
    * Retained only points inside built-up areas as urban samples.
    * Saved results as `builtup_hp.csv`.

    **Results:**

    * Matched urban points: 579,328
    * Match rate: 64.43%
    * Final sample: 328 cities/counties with >50 valid housing price records.


#### Rural Land Price Estimation

[rent_curve_cal_loc.ipynb](Code/rent_curve_cal_loc.ipynb)

We estimate average rural land prices at the city level using spatially disaggregated agricultural land rent data. The steps are as follows:

#### 1. **Match with Administrative Units**

* Match rural land rent values with corresponding provinces, cities, and counties;
* Standardize names to align with administrative shapefiles;
* Save city-level results in `Agr_lp/` and combine them into `agr_val_with_admin.csv`.

#### 2. **Exclude Urban Areas**

To ensure rural representativeness, we:

* Perform a spatial join with the 2020 built-up area shapefile;
* Remove all points within urban boundaries;
* Retain only data points outside built-up areas as rural land observations.

#### 3. **Compute Average Land Price**

* For cities with **5 or more rural data points**, calculate the average based on filtered rural observations;
* For cities with **fewer than 5 points**, use all available observations without filtering.

Land prices are computed using a perpetual rent model:

$$
P = \frac{R}{r}
$$

where:

* $P$ = rural land price (RMB/m²),
* $R$ = annual land rent (RMB/m²/year),
* $r = 0.05$ = discount rate.

The resulting average land prices for each city are saved in `avg_lp.csv`.

## Computation Workflow

*Core/Basic script: [rent_curve_loc.py](Code/rent_curve_loc.py)*

This section outlines the main steps for computing the welfare loss caused by urban expansion constraints, using fitted rent curves and rural land prices.


### **1. Rent Curve Fitting**

I estimate urban land prices by converting housing prices using a Cobb-Douglas production function:

$$
P_L = \alpha \cdot P_{\text{house}}
$$

Where $\alpha = 0.3$, based on Zhang and Jin (2012), reflecting the land share in production. I then regress land price on distance to city center using OLS: `fit_urban_land_rent_curve()`

This generates a city-specific **rent-distance curve**.


### **2. Computing the Theoretical Urban Boundary**

Using the fitted rent curve and the city’s rural land price (from `avg_lp.csv`), I solve for the distance where urban land rent equals rural land price — this defines the **theoretical urban boundary** `get_urban_rural_boundary_edge()`

If no city-level rural price is available, I use the provincial average.

### **3. Measuring the Urban Expansion Gap**

To capture how actual urban extent deviates from the theoretical circular city:

* Take the highest-priced location as the **city center**.
* Draw a circle of radius equal to the theoretical boundary.
* Sample 300 points along the **actual built-up edge** inside this circle.
* Compute the **average gap** between these points and the theoretical boundary:`mean_internal_radius_gap_op()`

This gap reflects the spatial constraint imposed by the policy.

### **4. Welfare Loss Estimation**

Using the average boundary gap and the OLS rent curve, I compute welfare loss:

* Approximate the loss as a **triangle area** bounded by the actual and theoretical boundaries.
* Also compute the **welfare loss ratio**, i.e., loss relative to total land rent in the unconstrained city:`compute_welfare_loss_triangle_linear()`


### **Supporting Functions**

* `find_shp_file()` – Locates the appropriate shapefile for the given province and city.


## Embarassingly Parallel Computation and Efficiency Comparison

The second part of the computation workflow involves applying the same procedure independently to each city, making this an *embarassingly parallel* problem. I implemented and compared two different parallelization strategies and evaluated their efficiency based on total runtime.

### Midway: Slurm Array

Since the computations for different cities are independent and do not require communication between nodes, I used a Slurm job array on Midway to parallelize the tasks. According to Midway's configuration, up to 200 tasks can be run in parallel.

All city-level tasks, specified by province and city, were written into [tasks.txt](Code/tasks.txt). I submitted the array job using [slurm_array](Code/slurm_array.sbatch). After execution, I retrieved each array job's runtime using:

```
sacct -j <job_id> -P > slurm_array_times.txt
```

Then, I used a [script](Code/time_cal.py) to read and summarize the runtime:

```
Total wall time: 00:01:23
```

### AWS Batch with EC2 Instances

I also implemented batch processing using multiple EC2 instances on AWS to compute welfare losses for cities in parallel.

The process was as follows:

1. **Create an S3 bucket** and upload the necessary files using [upload_to_s3](Code/upload_to_s3.py), including:

   * Computation script: [rent_curve.py](aws_run/Code/rent_curve.py)
   * Batch run script: [run_batch.sh](aws_run/Code/run_batch.sh)
   * Batch task lists: [batches_aws](aws_run/Code/batches_aws/)
   * Zipped data: `aws_run.zip`

2. **Launch EC2 instances** via [launch_ec2.py](Code/launch_ec2.py)
   This script accepts `--num_ec2` to control the number of instances launched. Each instance:

   * Installs required Python packages
   * Downloads scripts and data from S3
   * Executes [run_batch.sh](aws_run/Code/run_batch.sh) to sequentially process the cities listed in one batch file
     While individual instances process their batches serially, all instances run in parallel.

3. **Log batch-level execution times**:
   Each instance records its start and end time, uploads them to S3, and I use [time_cal_aws.py](Code/time_cal_aws.py) to calculate the total runtime:

```
Total runtime: 00:02:00
```

### Analysis

Despite using multiple EC2 instances, the AWS batch runtime was longer than the Slurm array approach—even **excluding** EC2 provisioning, initialization, and environment setup time. The reasons include:

1. **Granularity of Task Allocation**

   * **AWS EC2**: Each instance processes an entire batch (e.g., `batch_1.txt`), containing multiple cities. Since the cities are computed *sequentially* within each instance, total runtime is bottlenecked by the slowest city in each batch.
   * **Slurm Array**: Each task typically handles a single city, allowing maximum parallelism. The effect of variable runtimes across cities is minimized.

2. **Underutilization of EC2 Resources**
   The `t3.large` instance type has multiple CPU cores, but in the current implementation, each instance uses only one core. Future versions of the script can be optimized with multiprocessing to utilize all available cores and further reduce runtime.

