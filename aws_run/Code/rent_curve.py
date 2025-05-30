import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from geopy.distance import geodesic
import statsmodels.api as sm
import re
import numpy as np
import argparse
import math
import csv

def extract_prefix_city(filename):
    """
    extract the prefix city from a filename.
    For example, "Anyang2020_new_Urb.shp" -> "henan"
    """
    base = filename.lower().replace(".shp", "")
    match = re.match(r"([a-z]+)", base)
    return match.group(1) if match else base

def find_shp_file(prov, city, root_path="/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Data/Raw/China_BuiltUp_300kCities_2020"):
    prov_folder = os.path.join(root_path, prov)
    if not os.path.isdir(prov_folder):
        print(f"Warning: Province folder does not exist: {prov_folder}")
        return None

    shp_files = [f for f in os.listdir(prov_folder) if f.endswith(".shp")]
    city_lower = city.lower()
    prov_lower = prov.lower()
    print(city_lower, prov_lower)

    candidate_list = [f for f in shp_files if city_lower in f.lower()]
    
    if len(candidate_list) == 1:
        matched_file = os.path.join(prov_folder, candidate_list[0])
        return matched_file

    for f in candidate_list:
        city_prefix = extract_prefix_city(f)
        print(f"Checking file: {f}, extracted prefix: {city_prefix}")
        if city_prefix == city_lower:
            print(f"Prefix match among candidates: {f}")
            matched_file = os.path.join(prov_folder, f)
            return matched_file
    
    for f in shp_files:
        if prov_lower in f.lower():
            print(f"Fallback: matched by province name: {f}")
            return os.path.join(prov_folder, f)


    print(f"No matching file found for {prov}-{city}.")
    return None

def fit_urban_land_rent_curve(prov, city, alpha=0.3,
                               base_dir="/home/xinyu01/Final_project/Data/Cleaned/City_hp"):
    """
    Given a city and alpha, estimate urban land rent curve:
    1. Load city CSV
    2. Compute land_price = alpha * house_price
    3. Identify city center (highest land_price)
    4. Compute distances from each point to center
    5. Fit land_price ~ dist

    Returns: (fitted OLS model, processed DataFrame)
    """

    filename = f"{prov}-{city}_hp.csv"
    file_path = os.path.join(base_dir, filename)
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None, None


    df = pd.read_csv(file_path)
    if "价格" not in df.columns or "lng84" not in df.columns or "lat84" not in df.columns:
        print(f"Missing required columns in: {filename}")
        return None, None

 
    df = df.rename(columns={"价格": "house_price"})
    df["land_price"] = df["house_price"] * alpha


    max_idx = df["land_price"].idxmax()
    center_coord = (df.loc[max_idx, "lat84"], df.loc[max_idx, "lng84"])

    def calc_dist(row):
        return geodesic((row["lat84"], row["lng84"]), center_coord).km

    df["dist"] = df.apply(calc_dist, axis=1)
    df = df.dropna(subset=["land_price", "dist"])


    X = sm.add_constant(df["dist"])
    y = df["land_price"]
    model = sm.OLS(y, X).fit()

    return center_coord,model, df


def get_urban_rural_boundary_edge(prov, city, model,
                                   avg_lp_path="/home/xinyu01/Final_project/Data/Cleaned/avg_lp.csv"):
    """
    Given province and city, and a land rent regression model (land_price ~ dist),
    return the implied urban edge (distance where land_price equals rural land_price).

    Returns:
        float (edge in km) or None if not found
    """

    key = f"{prov}-{city}"
    df_lp = pd.read_csv(avg_lp_path)
    df_lp = df_lp.dropna(subset=["prov_city", "land_price"])

    match = df_lp[df_lp["prov_city"] == key]
    if not match.empty:
        rural_lp = match["land_price"].values[0]
    else:
        prov_matches = df_lp[df_lp["prov"] == prov]
        if not prov_matches.empty:
            rural_lp = prov_matches["land_price"].mean()
            print(f"Using provincial average land price for {prov}: {rural_lp:.4f}")
        else:
            print(f"No land price data available for {key} or {prov}")
            return None

    beta0 = model.params["const"]
    beta1 = model.params["dist"]

    if beta1 == 0:
        print("Warning: distance coefficient is zero.")
        return None

    edge = (rural_lp - beta0) / beta1
    return edge if edge > 0 else None


def mean_internal_radius_gap_op(prov, city, center, R,
    root_path="/home/xinyu01/Final_project/Data/Raw/China_BuiltUp_300kCities_2020",
    crs_metric="EPSG:3857", sample_count=300, simplify_tolerance=5.0):
    """
    Compute the average internal radius gap: the average of (R - d),
    where d is the distance from each sampled built-up area boundary point
    (within radius R) to the city center.

    Args:
        prov, city         : Province and city name, used to locate the shapefile.
        center             : City center coordinates (lon, lat).
        R                  : Theoretical urban-rural boundary radius (in meters).
        root_path          : Root directory containing provincial shapefiles.
        crs_metric         : Projected CRS for distance calculations (default: EPSG:3857).
        sample_count       : Total number of points to sample along all outer boundaries.
        simplify_tolerance : Tolerance for simplifying the polygon boundaries (in meters) to speed up sampling.

    Returns:
        Mean internal gap (in kilometers); returns 0.0 if no valid points found.
    """

    shp_path = find_shp_file(prov, city, root_path)
    if shp_path is None:
        raise FileNotFoundError(f"Shapefile not found: prov={prov}, city={city}")

    gdf = gpd.read_file(shp_path).to_crs(crs_metric)
    gdf = gdf[gdf.is_valid]
    builtup_union = gdf.geometry.union_all()
    
    Origin = gpd.GeoSeries([Point(center)], crs="EPSG:4326").to_crs(crs_metric).iloc[0]

    if builtup_union.geom_type == "Polygon":
        exteriors = [builtup_union.exterior.simplify(simplify_tolerance)]
    else:
        exteriors = [poly.exterior.simplify(simplify_tolerance) for poly in builtup_union.geoms]

    lengths = [ext.length for ext in exteriors]
    total_length = sum(lengths)
    if total_length == 0:
        return 0.0

    all_pts = []
    for ext, L in zip(exteriors, lengths):
        n = max(int(sample_count * (L / total_length)), 1)
        dists = np.linspace(0, L, n, endpoint=False)
        all_pts.extend([ext.interpolate(d) for d in dists])

    gaps = []
    for pt in all_pts:
        d = pt.distance(Origin)
        if d <= R:
            gaps.append(R - d)

    return float(np.mean(gaps)) / 1000 if gaps else 0.0

def compute_welfare_loss_triangle_linear(model, edge_km, gap_km):
    """
    Compute the welfare loss as the area of a triangle under a linear rent curve:
      r(d) = beta_0 + beta_1 * d

    Args:
      model    : Fitted statsmodels OLS result (regression of price on distance)
      edge_km  : Theoretical urban boundary radius (in kilometers)
      gap_km   : Average distance between theoretical and actual urban boundary (in kilometers)

    Returns:
      loss     : Welfare loss triangle area (in price * km units)
    """
    dist_a = edge_km - gap_km
    if dist_a <= 0:
        return 0.0

    slope_name = [n for n in model.params.index if n != "const"][0]
    beta1 = model.params[slope_name]

    loss = -0.5 * beta1 * gap_km**2

    S = 0.5 * edge_km * (- beta1 * edge_km)

    loss_ratio = loss / S if S != 0 else 0.0

    return loss,loss_ratio



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prov", type=str, required=True)
    parser.add_argument("--city", type=str, required=True)
    args = parser.parse_args()

    prov = args.prov
    city = args.city
    alpha = 0.3


    (center_lat, center_lon), model, df = fit_urban_land_rent_curve(prov, city, alpha)
    if model is not None:
        print(f"Fitted model for {prov}-{city}:")
        print(model.summary())

        edge_km = get_urban_rural_boundary_edge(prov, city, model)
        if edge_km is not None:
            print(f"Urban-rural boundary edge for {prov}-{city}: {edge_km:.2f} km")

            gap_km = mean_internal_radius_gap_op(prov, city, (center_lon,center_lat), edge_km * 1000)
            print(f"Average internal radius gap: {gap_km:.2f} km")

            loss, loss_ratio = compute_welfare_loss_triangle_linear(model, edge_km, gap_km)
            print(f"Welfare loss: {loss:.2f}, Loss ratio: {loss_ratio*100:.2f}%")
        else:
            print("Could not determine urban-rural boundary edge.")
    else:
        print("Model fitting failed.")
        

    output_file = "/home/xinyu01/Final_project/Data/welfare_results_aws.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)


    loss_val = loss if 'loss' in locals() else None
    loss_ratio_val = loss_ratio if 'loss_ratio' in locals() else None


    if loss_val not in [None, 0] and loss_ratio_val not in [None, 0]:
        with open(output_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([prov, city, loss_val, loss_ratio_val * 100])

