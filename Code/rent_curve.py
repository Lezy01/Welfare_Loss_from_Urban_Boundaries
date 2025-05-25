#!/usr/bin/env python3
"""
city_parallel_pipeline.py

并行处理每个城市的数据：
1. 农村土地产值转化为租金，并计算农村区域平均地租
2. 拟合地租曲线（Rent curve）
3. 计算三角形福利损失（TODO）

使用 joblib 在本地多核并行执行。

依赖:
    geopandas, pandas, numpy, statsmodels, joblib
"""
import os
import glob
import pandas as pd
import geopandas as gpd
import numpy as np
import statsmodels.api as sm
from joblib import Parallel, delayed

# === 配置路径 ===
HOUSE_PRICE_DIR = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Data/Cleaned/City_hp"
ADMIN_SHP = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Data/Raw/China_Adm_2020/China2020County.shp"
BUILTUP_SHP = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Data/Cleaned/China_BuiltUp_300kCities_2020/China_BuiltUp_300kCities_2020.shp"
RURAL_CSV = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Data/Raw/Gaze_v4_agvalue_2022.csv"  # 假设转化后的农业产值CSV
OUTPUT_FILE = "/Users/yxy/UChi/Spring2025/MACS30123/Final_project/Results/city_results.csv"

# === 功能函数 ===

def convert_ag_value_to_rent(df):
    """
    将农业产值转换为农地租金。示例中直接使用产值作为租金，可根据实际逻辑调整。
    """
    df['rent'] = df['value']  # TODO: 按 CPI 或农产品价格指数调整
    return df


def compute_rural_rent_mean(city_name, admin_gdf, builtup_gdf, rural_csv):
    """
    对指定城市，提取行政区-建成区差集（农村区域），
    空间匹配农业产值点，计算平均地租。
    """
    # 1. 读取农业产值数据
    df_rural = pd.read_csv(rural_csv)
    gdf_rural = gpd.GeoDataFrame(df_rural,
                                 geometry=gpd.points_from_xy(df_rural.lng, df_rural.lat),
                                 crs="EPSG:4326")
    # 2. 筛选行政区和建成区多边形
    admin_city = admin_gdf[admin_gdf['NAME_2'].str.lower() == city_name.lower()]
    builtup_city = builtup_gdf[builtup_gdf['city'].str.lower() == city_name.lower()]
    # 3. 差集 -> 农村多边形
    rural_area = gpd.overlay(admin_city, builtup_city, how='difference')
    # 4. 空间匹配
    gdf_rural_in = gpd.sjoin(gdf_rural, rural_area, how='inner', predicate='within')
    # 5. 转换 rent & 计算均值
    gdf_rural_in = convert_ag_value_to_rent(gdf_rural_in)
    return gdf_rural_in['rent'].mean()


def fit_rent_curve(city_name, hp_csv, admin_gdf, builtup_gdf):
    """
    对指定城市，读取房价数据，估算 alpha，计算地租并拟合 rent curve。
    返回 alpha, (intercept, slope)
    """
    # 1. 读取房价文件
    df_hp = pd.read_csv(hp_csv)
    gdf_hp = gpd.GeoDataFrame(df_hp,
                              geometry=gpd.points_from_xy(df_hp.lng84, df_hp.lat84),
                              crs="EPSG:4326")
    # 2. OLS 回归 ln(price) ~ ln(FAR) + 区县固定效应
    df_hp = df_hp.dropna(subset=['容积率', '价格', '区县'])
    df_hp['ln_price'] = np.log(df_hp['价格'])
    df_hp['ln_far'] = np.log(df_hp['容积率'])
    # 构建哑变量
    dummies = pd.get_dummies(df_hp['区县'], prefix='county', drop_first=True)
    X = pd.concat([df_hp['ln_far'], dummies], axis=1)
    X = sm.add_constant(X)
    model = sm.OLS(df_hp['ln_price'], X).fit()
    beta_far = model.params['ln_far']
    alpha = max(-beta_far, 0.3)
    # 3. 计算地租
    gdf_hp['rent'] = gdf_hp['价格'] * alpha
    # 4. 城市中心 & 距离
    center_pt = gdf_hp.loc[gdf_hp['价格'].idxmax()].geometry
    gdf_hp['distance'] = gdf_hp.geometry.distance(center_pt)
    # 5. 拟合 rent curve: ln(rent) ~ distance
    df2 = gdf_hp.dropna(subset=['rent', 'distance'])
    df2['ln_rent'] = np.log(df2['rent'])
    X2 = sm.add_constant(df2['distance'])
    res2 = sm.OLS(df2['ln_rent'], X2).fit()
    return alpha, res2.params['const'], res2.params['distance']


def process_city(hp_csv):
    """
    单城市完整处理流程，返回结果字典。
    """
    city_name = os.path.basename(hp_csv).replace('_hp.csv', '')
    # 读取共享的数据
    admin_gdf = gpd.read_file(ADMIN_SHP).to_crs("EPSG:4326")
    builtup_gdf = gpd.read_file(BUILTUP_SHP).to_crs("EPSG:4326")
    # 1. 农村平均租金
    rural_mean = compute_rural_rent_mean(city_name, admin_gdf, builtup_gdf, RURAL_CSV)
    # 2. 地租曲线拟合
    alpha, intercept, slope = fit_rent_curve(city_name, hp_csv, admin_gdf, builtup_gdf)
    return {
        'city': city_name,
        'alpha': alpha,
        'rent_curve_intercept': intercept,
        'rent_curve_slope': slope,
        'rural_rent_mean': rural_mean
    }


def main():
    # 所有城市房价文件
    hp_files = glob.glob(os.path.join(HOUSE_PRICE_DIR, '*_hp.csv'))
    # 并行处理（根据实际核数调整 n_jobs）
    results = Parallel(n_jobs=8)(delayed(process_city)(f) for f in hp_files)
    df_res = pd.DataFrame(results)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df_res.to_csv(OUTPUT_FILE, index=False)
    print("All cities processed. Results saved to:", OUTPUT_FILE)

if __name__ == '__main__':
    main()
