# Este fue uno de los primeros scripts que hice para validar las formulas de Tableau (Antes de que existiera ChatgPT).
# El objetivo de este script es validar las columnas de un archivo exportado desde Tableau,
# asegurando que las columnas contengan la cantidad correcta de semanas y que las semanas de inicio
# sean las esperadas.
# El script se basa en un archivo de Excel exportado desde Tableau, el cual contiene datos
# organizados en columnas que representan diferentes periodos de tiempo, como semanas, semestres,
# y años. El script recorre estas columnas y cuenta la cantidad de semanas que tienen valores
# distintos de cero, verificando si coinciden con las expectativas definidas en una lista de
# periodos a evaluar.

# Este script causo un gran impacto en terminos de tiempo que se realizaba al validar las formulas de Tableau,
# ya que antes se realizaba de forma manual y con este script se automatizo el proceso
# Reduciendo horas de trabajo a solo minutos.

import pandas as pd

def contador_semanas (columna,semana_eval):
    #Aqui lo que hacemos es extraer todas las columnas y recorrelas cada una de ellas
    columnas = df.columns.tolist()
    for col in columnas:
        contador = 0
        if 'SUMA' in col:
            #Aqui columna toma un valor especifico dado en la lista de abajo el cual veremos si las columnas tienen semestre, -2, etc.
            if columna in col:
                ##print('Esta columna debería mostrar {} semanas'.format(columna))
                #Aqui lo que hacemos es contar la cantidad de filas que tienen valores distintos de 0
                largo=len(df)
                for i in range(0,largo):
                    val = df.loc[i, col]
                    if val != 0:
                        contador = contador +1
                        #Ahora como ultimo paso lo haremos aca adentro para ver en que semana termina de contar
                        if columna == '52' and contador == 52:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == 'Semestre' and contador == 24:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == '12' and contador == 12:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == 'Ult. 3' and contador == 3:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == 'Ult. 4' and contador == 4:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == 'Ult. 5' and contador == 5:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == '-1' and contador == 1:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval-1:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == '-2' and contador == 1:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval-2:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == '-3' and contador == 1:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval-3:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == 'YTD' and contador == semana_eval:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                        elif columna == 'Ult. año (n) v' and contador == 1:
                            agno = df.iloc[i, 0]
                            semana = df.iloc[i, 1]
                            if semana == semana_eval:
                                print('Semana: OK')
                            else:
                                print('!!ERROR!! En la columna {} AL PARTIR EN LA SEMANA'.format(columna))
                ##print(contador)
                #Aqui lo que haremos es imprimir si tiene exito o un error al contar las columnas
                if columna == '52':
                    if contador == 52:
                        print('Cantidad: OK')
                    else:
                        print('!!!!!!ERROR!!!!!!, LA COLUMNA {} TIENE {} SEMANAS'.format(columna,contador))
                elif columna == 'Semestre':
                    if contador == 24:
                        print('Cantidad: OK')
                    else:
                        print('!!!!!!ERROR!!!!!!, LA COLUMNA {} TIENE {} SEMANAS'.format(columna,contador))
                elif columna == '12':
                    if contador == 12:
                        print('Cantidad: OK')
                    else:
                        print('!!!!!!ERROR!!!!!!, LA COLUMNA {} TIENE {} SEMANAS'.format(columna,contador))
                elif columna == 'Ult. 3':
                    if contador == 3:
                        print('Cantidad: OK')
                    else:
                        print('!!!!!!ERROR!!!!!!, LA COLUMNA {} TIENE {} SEMANAS'.format(columna,contador))
                elif columna == 'Ult. 4':
                    if contador == 4:
                        print('Cantidad: OK')
                    else:
                        print('!!!!!!ERROR!!!!!!, LA COLUMNA {} TIENE {} SEMANAS'.format(columna,contador))
                elif columna == 'Ult. 5':
                    if contador == 5:
                        print('Cantidad: OK')
                    else:
                        print('!!!!!!ERROR!!!!!!, LA COLUMNA {} TIENE {} SEMANAS'.format(columna,contador))
                elif columna == '-1' or columna == '-2' or columna == '-3':
                    if contador == 1:
                        print('Cantidad: OK')
                    else:
                        print('!!!!!!ERROR!!!!!!, LA COLUMNA {} TIENE {} SEMANAS'.format(columna,contador))
                elif columna == 'YTD':
                    if contador == semana_eval:
                        print('Cantidad: OK')
                    else:
                        print('!!!!!!ERROR!!!!!!, LA COLUMNA {} TIENE {} SEMANAS'.format(columna,contador))
                elif columna == 'Ult. año (n) v':
                    if contador == 1:
                        print('Cantidad: OK')
                    else:
                        print('!!!!!!ERROR!!!!!!, LA COLUMNA {} TIENE {} SEMANAS'.format(columna,contador))

archivos=['',' (2)',' (3)',' (4)',' (5)',' (6)',' (7)',' (8)',' (9)',' (10)',' (11)',' (12)',' (13)']
semana_eval = int(input('Ingrese la semana a evaluar: '))
lista = ['52','Semestre','12','Ult. 3','Ult. 4','Ult. 5','-1','-2','-3','YTD','Ult. año (n) v']
#df  = pd.read_excel('C:/Users/Mario Sanchez/Downloads/ExportAll (1).xlsx')
for archiv in archivos:
    df  = pd.read_excel('C:/Users/Mario Sanchez/Downloads/ExportAll{}.xlsx'.format(archiv))
    #print('La cantiadad de columnas es de ',len(df.columns.tolist()))
    for i in lista:
        contador_semanas (i,semana_eval)
    print('Todo OK! archivo {}'.format(archiv))
