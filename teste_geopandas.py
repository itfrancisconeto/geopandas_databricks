# Databricks notebook source
# DBTITLE 1,Instalação dos pacotes:
pip install geopandas

# COMMAND ----------

pip install folium

# COMMAND ----------

# DBTITLE 1,Importação das bibliotecas:
import urllib
import os, zipfile, shutil
import tarfile
import geopandas as gpd
import pandas as pd
import folium
from folium.plugins import HeatMap
from platform import python_version
from shutil import copyfile
print(python_version())

# COMMAND ----------

# DBTITLE 1,Fontes dos dados:
https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2020/Brasil/BR/
https://opendatasus.saude.gov.br/dataset/registro-de-ocupacao-hospitalar/resource/f9391f7c-9775-4fac-a3ce-bf384e2674c2

# COMMAND ----------

# DBTITLE 1,Consumo dos dados:
def download_file(url):
  filename = url.split('/')[-1] 
  response = urllib.request.urlopen(url)
  content = response.read()
  with open(filename, 'wb' ) as f:
      f.write( content )  
  return filename

# COMMAND ----------

# shutil.rmtree('/tmp', ignore_errors=True)

# COMMAND ----------

ls

# COMMAND ----------

if not os.path.isfile('BR_Municipios_2020.zip'):
  download_file('https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2020/Brasil/BR/BR_Municipios_2020.zip')

# COMMAND ----------

with zipfile.ZipFile("BR_Municipios_2020.zip", 'r') as zip_ref:
    zip_ref.extractall("tmp")

# COMMAND ----------

if not os.path.isfile('esus-vepi.LeitoOcupacao.csv'):
  download_file('https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos/2021-07-27/esus-vepi.LeitoOcupacao.csv')

# COMMAND ----------

shutil.move("esus-vepi.LeitoOcupacao.csv", "/tmp/esus-vepi.LeitoOcupacao.csv")

# COMMAND ----------

shp = gpd.read_file('tmp/BR_Municipios_2020.shp')

# COMMAND ----------

leitoOcupacao = pd.read_csv('/tmp/esus-vepi.LeitoOcupacao.csv', low_memory=False)

# COMMAND ----------

type(leitoOcupacao)

# COMMAND ----------

type(shp)

# COMMAND ----------

# DBTITLE 1,Tratamento dos dados:
shp.columns

# COMMAND ----------

columns_to_drop = ['CD_MUN', 'SIGLA_UF', 'AREA_KM2']
shp.drop(columns_to_drop, axis = 1, inplace = True)

# COMMAND ----------

shp.tail()

# COMMAND ----------

shp.plot()

# COMMAND ----------

leitoOcupacao.head(5)

# COMMAND ----------

columns_to_drop = ['_id', 'dataNotificacao', 'cnes', 'ocupacaoSuspeitoCli',
       'ocupacaoSuspeitoUti', 'saidaSuspeitaObitos', 'saidaSuspeitaAltas', 'saidaConfirmadaObitos',
       'saidaConfirmadaAltas', 'origem', '_p_usuario', 'estadoNotificacao',
       'municipioNotificacao', 'excluido', 'validado', '_created_at', '_updated_at', 'estado']
leitoOcupacao.drop(columns_to_drop, axis = 1, inplace = True)

# COMMAND ----------

leitoOcupacao.head(5)

# COMMAND ----------

leitoOcupacao['ocupacaoPorMunicipio'] = leitoOcupacao['ocupacaoConfirmadoCli']+leitoOcupacao['ocupacaoConfirmadoUti']

# COMMAND ----------

leitoOcupacao.drop(['ocupacaoConfirmadoCli', 'ocupacaoConfirmadoUti'], axis = 1, inplace = True)

# COMMAND ----------

leitoOcupacao.shape

# COMMAND ----------

nans = leitoOcupacao.isna().sum()
nans[nans > 0]

# COMMAND ----------

leitoOcupacao.dropna(subset = ['municipio', 'ocupacaoPorMunicipio'], inplace = True)

# COMMAND ----------

leitoOcupacao.shape

# COMMAND ----------

leitoOcupacao.head(5)

# COMMAND ----------

leitoOcupacaoMapa = pd.merge(shp, leitoOcupacao, left_on='NM_MUN', right_on='municipio', how='left')

# COMMAND ----------

leitoOcupacaoMapa.shape

# COMMAND ----------

nans = leitoOcupacaoMapa.isna().sum()
nans[nans > 0]

# COMMAND ----------

leitoOcupacaoMapa.dropna(subset = ['municipio', 'ocupacaoPorMunicipio'], inplace = True)

# COMMAND ----------

# DBTITLE 1,Definição do centroid para o OpenStreetMap:
leitoOcupacaoMapa = leitoOcupacaoMapa.set_crs("EPSG:3395", allow_override=True)

# COMMAND ----------

y = leitoOcupacaoMapa.centroid.y.iloc[0]

# COMMAND ----------

y

# COMMAND ----------

x = leitoOcupacaoMapa.centroid.x.iloc[0]

# COMMAND ----------

x

# COMMAND ----------

data = []
for i in range(len(leitoOcupacaoMapa)):
    data.append([leitoOcupacaoMapa['geometry'].iat[i].centroid.y,
                 leitoOcupacaoMapa['geometry'].iat[i].centroid.x,
                 leitoOcupacaoMapa['ocupacaoPorMunicipio'].iat[i]/leitoOcupacaoMapa['ocupacaoPorMunicipio'].max()])

# COMMAND ----------

base = folium.Map([y, x], zoom_start=11, tiles='OpenStreetMap')
HeatMap(data, name="Casos Confirmados").add_to(base)
folium.LayerControl().add_to(base)
base.save('HeatMap.html')
base

# COMMAND ----------

ls -l

# COMMAND ----------

# zipObj = zipfile.ZipFile('HeatMap.zip', 'w')
# zipObj.write('HeatMap.html')
# zipObj.close()

tar = tarfile.open("HeatMap.tar", "w")
for name in ["HeatMap.html"]:
    tar.add(name)
tar.close()

# COMMAND ----------

shutil.move("HeatMap.tar", "/tmp/HeatMap.tar")

# COMMAND ----------

ls -l /tmp

# COMMAND ----------

dbutils.fs.cp("file:/tmp/HeatMap.tar", "/FileStore/HeatMap.tar")

# COMMAND ----------

# DBTITLE 0,Url para baixar o arquivo final:
https://community.cloud.databricks.com/files/HeatMap.tar?o=4313936170029748
