# Your code here
import pandas as pd

df = pd.read_excel(r'.\data\vendas-combustiveis-m3.xls', sheet_name="Derivative")
df_diesel = pd.read_excel(r'.\data\vendas-combustiveis-m3.xls', sheet_name="Diesel")

group_derivative = df.groupby(['ESTADO','COMBUSTÍVEL']) # Sales of oil derivative fuels by UF and product

#Sales of diesel by UF and type
# print(group_one.first())

group_diesel = df_diesel.groupby(['ESTADO','COMBUSTÍVEL'])

diesel_year_month = df_diesel.loc[df_diesel['ANO'].isin([2013])]

s = diesel_year_month.stack(0).to_frame()
print(s)