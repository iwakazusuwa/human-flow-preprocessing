# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
import numpy as np
import os


#=============================================
# ファイル・フォルダ情報の設定
#=============================================
INPUT_FILENAME = "人流データ.csv"
INPUT_FOLDER = "2_data"
OUTPUT_FILENAME = "データ縦持ち整形.csv"
OUTPUT_WITH_DIST_FILENAME = "データ縦持ち整形_距離追加.csv"
OUTPUT_FOLDER = "3_output"


# %%
#=============================================
# パスの取得（OS依存しない方法）
#=============================================
current_dir = os.getcwd()
#print("INFO: カレントパス:", current_dir)

parent_dir = os.path.dirname(current_dir)
#print("INFO: パレントパス:", parent_dir)

input_path = os.path.join(parent_dir, INPUT_FOLDER, INPUT_FILENAME)
#print("INFO: 入力データファイルパス:", input_path)

output_fol_path = os.path.join(parent_dir, OUTPUT_FOLDER)
os.makedirs(output_fol_path, exist_ok=True)

output_path = os.path.join(parent_dir, OUTPUT_FOLDER, OUTPUT_FILENAME)
output_WITH_path = os.path.join(parent_dir, OUTPUT_FOLDER, OUTPUT_WITH_DIST_FILENAME)
#print("INFO: 出力フォルダパス:", output_fol_path)


# CSV読み込み（UTF-8 BOM付き想定）
df = pd.read_csv(input_path, encoding="utf-8-sig")

# %%
# データを縦持ちにする
rows = []

for _, row in df.iterrows():
    detect_num = int(row["検知数"])
    time_step = row["time_step"]
    # 検知数分だけ展開
    for i in range(detect_num):
        # 列名は id, x, y または id_1, x_1, y_1 ...
        id_col = "id" if i == 0 else f"id_{i}"
        x_col  = "x"  if i == 0 else f"x_{i}"
        y_col  = "y"  if i == 0 else f"y_{i}"
        rows.append([
            detect_num,
            time_step,
            row[id_col],
            row[x_col],
            row[y_col]
        ])

# DataFrame化
long_df = pd.DataFrame(rows, columns=["検知数", "time_step", "id", "x", "y"])
# 保存
long_df.to_csv(output_path, index=False, encoding="utf-8-sig")

# %%
#　idのデータを取り出して、time_stepソート、移動距離追加

df = long_df

rows = []
for _, r in df.iterrows():
    detect_num = int(r["検知数"])
    ts = r["time_step"]
    for i in range(detect_num):
        id_col = "id" if i == 0 else f"id_{i}"
        x_col  = "x"  if i == 0 else f"x_{i}"
        y_col  = "y"  if i == 0 else f"y_{i}"
        if id_col in df.columns and pd.notna(r.get(id_col, np.nan)):
            rows.append([
                r["検知数"],
                ts,
                r[id_col],
                r.get(x_col, np.nan),
                r.get(y_col, np.nan),
            ])

long_df = pd.DataFrame(rows, columns=["検知数", "time_step", "id", "x", "y"])

# 数値変換
long_df["time_step"] = pd.to_numeric(long_df["time_step"], errors="coerce")
long_df["id"] = pd.to_numeric(long_df["id"], errors="coerce")
long_df["x"] = pd.to_numeric(long_df["x"], errors="coerce")
long_df["y"] = pd.to_numeric(long_df["y"], errors="coerce")

# グループごとに time_step でソートして距離計算
def calc_distance(g):
    g = g.sort_values("time_step")
    g["distance"] = np.sqrt((g["x"] - g["x"].shift())**2 + (g["y"] - g["y"].shift())**2)
    g["distance"] = g["distance"].fillna(0)
    return g

long_df = long_df.groupby("id").apply(calc_distance).reset_index(drop=True)

# 出力保存
long_df.to_csv(output_WITH_path, index=False, encoding="utf-8-sig")

os.startfile(os.path.realpath(output_fol_path))

