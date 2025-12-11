Hola!, este es un tutorial para hacer funcionar el codigo, recordar que el csv que contiene la informacion de los delitos realizados en chile 
esta en formato .parquet ya que el csv sobrepasaba los 25 mb permitidos y es mas simple subirlo en .parquet y luego transformarlo por su cuenta 
a csv, pero no te preocupes!!, hay un codigo que te permite transformar de parquet a csv en el repositorio, su nombre es Parquet a Csv.py.

Teniendo eso en cuenta, los csv a ocupar serian poblacion.csv y output.csv (el parquet transformado a csv), y estos deben estar dentro de la 
carpeta en donde tambien tienes los codigos que se deben descargar.

-cargar_final_2025.py
-app_interactiva.py
-analisis_final.py

Esos serian los 3 codigos que se necesitan para poder realizar todo lo que el informe muestra. Recordar que en cargar_final_2025 hay una variable
llamada path_archivo en donde debes escribir la raiz del archivo poblacion.csv.

Recordar tambien que en el archivo app_interactiva.py si no escribes bien las netradas como Temuco, y escribes teumco, no te dara ningun
resultado, asi que hay que estar atento a eso tambien.

Finalmente, el archivo analisis_final.py te crea un grafico para poder visualizar de manera mas atractiva los datos de las 5 regiones mas 
peligrosas de chile a partir de cantidad de delitos por 100k de habitantes.
