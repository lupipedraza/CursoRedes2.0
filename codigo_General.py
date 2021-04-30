# -*- coding: utf-8 -*-

"""
En las siguientes líneas, se definirán las principales clases y funciones a
utilizar en la descarga de datos de la red social twitter, así también como
algunas cuestiones referidas a pre-procesamiento y análisis de los datos

Estas funciones pueden ser llamadas desde otros archivos que estén en la misma carpeta, 
cuando comiencen con la linea
from codigo_General import FuncionAUsar
"""

# ------ Algunas de las librerías que se utilizarán -----------
'''
Algunas de estas librerías son necesario instalar previamente. 
Para eso en la consola correr
!pip install LibreriaFaltante

'''

from tweepy.streaming import StreamListener
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import timedelta
import json
import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from matplotlib_venn import venn3
from wordcloud import WordCloud
import re
import codecs 
import networkx as nx
import community
import string
import os
from os import path
import seaborn as sbn
import nltk
nltk.download('stopwords')
nltk.download('punkt')


#------------Funciones auxiliares--------------

# -------------- Defino una función para sacar tildes de las palabras ---------------------------
def saca_tildes(string):
    '''
    Función auxiliar que se usa en varias funciones para cambiar todas las tildes por su vocal sin tilde
    '''
    string = string.replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u')
    return string

# -------------- Defino una función que lea las claves desde un .txt (por privacidad)-------------

def lector_claves(archivo_claves = 'claves_Twitter.txt'):
    """
    Ingresamos con el nombre del archivo y
    nos devuelve las claves en una lista. Cada elemento corresponde, respectivamente, a:
        CONSUMER_KEY
        CONSUMER_SECRET
        ACCESS_TOKEN
        ACCES_TOKEN_SECRET
    Por default, se define el nombre del archivo de entrada como "claves_Twitter.txt", de forma tal 
    que lo único que hay que hacer es crear ese archivo por única vez con los datos de las claves
    """
    with open(archivo_claves, 'r') as f:
        claves = f.read().split('\n')
    return claves # Variable de salida, las claves ordenadas

def nube_palabras(textos_completos, archivo_imagen = ''):
            """
            La idea de esta función es darle los datos, un rango de fechas, y que 
            nos devuelva la nube de palabras asociada a la discusión durante esas fechas.
            """

            # Incorporamos todos los textos de los tweets a textos

            textos = []
            for t in textos_completos:
                textos.append(re.sub(r'https?:\/\/\S*', '', t, flags=re.MULTILINE).lower())

            es_stop = nltk.corpus.stopwords.words('spanish')
            
            #Filtramos las stopwords y sacamos los .,' y tildes
            
            textos = ''.join(textos).replace(',',' ').replace('.',' ').replace("'",' ').split(' ')
            
            textos_filtrado = list(filter(lambda x: x not in es_stop, textos))
            textos = ' '.join(textos_filtrado)
            textos = saca_tildes(textos)
    
            # Armamos la wordcloud
            wc = WordCloud(width=1600,
                           height=800,
                           background_color = "white",
                           contour_width = 3,
                           contour_color = 'steelblue',
                           max_words = 100,
                           collocations=False).generate_from_text(textos)
            sbn.set_context("paper", font_scale = 2)

            plt.figure(figsize = (10,8), dpi = 100)
            plt.title('Nube de Palabras', fontsize = 20)
            plt.imshow(wc, interpolation='bilinear')
            plt.axis("off")
            if archivo_imagen=="":
                plt.show()
            else:
                plt.savefig(archivo_imagen,bbox_inches='tight')
                plt.show()
            plt.clf()            
            sbn.reset_orig()

#------------FUNCIONES DE DESCARGA -----------
# -------------- Defino las clases y funciones que darán lugar al streamer ------------

class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list, languages):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(lector_claves()[0], lector_claves()[1])
        auth.set_access_token(lector_claves()[2], lector_claves()[3])
        
        stream = Stream(auth, listener, tweet_mode = 'extended')
        # This line filter Twitter Streams to capture data by the keywords: 
        if len(hash_tag_list) != 0:
            stream.filter(languages = languages,
                          track = hash_tag_list,
                          )
        else:
            stream.sample(languages = languages)
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True
    def on_error(self, status):
        print(status)

#------------------------ Defino función de pesca de la actividad de un determinado usuario ----------------------

def pescar_palabras(Palabras,Archivo_Tweets,Cantidad=100):
    
    '''
    Esta función busca descargar y guardar en un archivo todos los tweers (originales, QT o RT) de una lista de usuarios
    
    '''
    
    #Se definen las clavesy los accesos
    auth = OAuthHandler(lector_claves()[0], lector_claves()[1])
    auth.set_access_token(lector_claves()[2], lector_claves()[3])
    api = tweepy.API(auth, wait_on_rate_limit=True)

    Busqueda=Palabras[0]
    for p in Palabras[1:]:
        Busqueda+=' OR '+ p
        
        #Descargar todos los tweets (Cantidad)
    Tweets = tweepy.Cursor(api.search,q=Busqueda,tweet_mode = 'extended').items(Cantidad)
        
        #Guardarlos en el archivo
    for tweet in Tweets:
            tweet = tweet._json
            with open(Archivo_Tweets, 'a') as tf:
                json.dump(tweet,tf)
                tf.write('\n\n')
                                
#------------------------ Defino función de pesca de la actividad de un determinado usuario ----------------------

def pescar_usuarios(Usuarios,Archivo_Tweets,Cantidad=100):
    
    '''
    Esta función busca descargar y guardar en un archivo todos los tweers (originales, QT o RT) de una lista de usuarios
    
    '''
    
    #Se definen las clavesy los accesos
    auth = OAuthHandler(lector_claves()[0], lector_claves()[1])
    auth.set_access_token(lector_claves()[2], lector_claves()[3])
    api = tweepy.API(auth, wait_on_rate_limit=True)


    for usuario in Usuarios: #Para cada usuario
        
        #Descargar todos los tweets (Cantidad)
        Tweets = tweepy.Cursor(api.user_timeline,screen_name=usuario,tweet_mode = 'extended',excluded_replies=True).items(Cantidad)
        
        #Guardarlos en el archivo
        for tweet in Tweets:
            tweet = tweet._json
            with open(Archivo_Tweets, 'a') as tf:
                json.dump(tweet,tf)
                tf.write('\n\n')

#----------------------        

        
class Bases_Datos():
    
    def __init__(self):
        
        self.tweets=pd.DataFrame() 
        self.usuarios=pd.DataFrame() 
 
        self.grafo=nx.Graph()
        self.grafo_menciones=nx.Graph()
        self.grafo_hashtags=nx.Graph()
 

    def cargar_datos(self,archivo_datos):
        datos = pd.read_csv(archivo_datos,
                            sep = ',',
                            error_bad_lines = False,
                            dtype = {'id_nuevo' : object,
                                     'id_original' : object},
                            parse_dates = ['fecha_original','fecha_nuevo'],
                            date_parser = pd.to_datetime)
        self.tweets=datos 
        
    def cargar_usuarios(self,archivo_usuarios):
        with open(archivo_usuarios) as json_file:
             datos_usuarios= json.load(json_file)
        self.usuarios=datos_usuarios

    def guardar_datos(self,archivo_datos):
        self.tweets.to_csv(archivo_datos) 
        
    def guardar_usuarios(self,archivo_usuarios):
        with open(archivo_usuarios, 'w') as outfile:
            json.dump(self.usuarios, outfile)


    # Analisis Estadisticos
    def plot_rol_usuario(self,archivo_imagen=''): # Rol de los usuarios a partir de los datos preprocesados 

        usuarios_generadores = set(self.tweets.usuario_original.values) #Conjunto de usuarios que tienen tweets originales
        usuarios_retweeteadores = set(self.tweets[self.tweets.relacion_nuevo_original == 'RT'].usuario_nuevo.values) #Conjunto de usuarios que retwitearon tweets
        usuarios_citadores = set(self.tweets[self.tweets.relacion_nuevo_original == 'QT'].usuario_nuevo.values) #Conjunto de usuarios que citaron
        
        total_usuarios = len(usuarios_generadores.union(usuarios_retweeteadores).union(usuarios_citadores)) #Cantidad total de todos los usuarios
        
        
        #Realizar la la figura
        sbn.set_context("paper", font_scale = 1.5)        
        plt.figure(figsize = (11,8), dpi = 300) 
        plt.title('Rol de los Usuarios', fontsize = 20)
        v = venn3([usuarios_retweeteadores, usuarios_generadores, usuarios_citadores],
                  set_labels = ('Lxs que retweetean', 'Lxs que generan', 'lxs que citan'),
                  )
        for indice in ['100','110','101','010','011','001','01','10','11','111']:
            try:
                v.get_label_by_id(indice).set_text('{}%'.format(round(int(v.get_label_by_id(indice).get_text())*100/total_usuarios,1)))
            except:
                pass
        plt.text(-.05,-.65,'El total de usuarios registrados durante el período fue de : {}'.format(total_usuarios))

        if archivo_imagen=='':
            plt.show()
        else:
            plt.savefig(archivo_imagen,bbox_inches='tight')
            plt.show()
        
        plt.clf()
        sbn.reset_orig()


    def plot_tipo_tweet(self, archivo_imagen = ''):
        """
        Esta función toma como entrada el archivo de datos preprocesados y
        devuelve una imagen tipo diagrama de Venn con el tipo de tweets pescados
        """
        #Levantar los datos preprocesados del archivo
    
        originales = set(self.tweets.id_original.values) #Conjunto de tweets originales
        rt = set(self.tweets[self.tweets.relacion_nuevo_original == 'RT'].id_nuevo.values) #Conjunto de retweets
        citas = set(self.tweets[self.tweets.relacion_nuevo_original == 'QT'].id_nuevo.values) #Conjunto de citas    
        total_tweets = len(originales.union(rt).union(citas)) #Cantidad total de tweets
        
        # Realizar la figura
        labels = ['RT', 'Originales', 'QT'] 
        sizes = [100 * len(rt) / total_tweets, 100 * len(originales) / total_tweets, 100 * len(citas) / total_tweets]
        
        sbn.set_context("paper", font_scale = 1.5)
        
        plt.figure(figsize = (11,8), dpi = 300)
        plt.title('Tipos de Tweets', fontsize = 20)
        plt.pie(sizes, autopct='%1.1f%%')
        plt.legend(labels)
        plt.axis('equal')
        plt.text(-.1,-1.2,'El total de tweets registrados durante el período fue de : {}'.format(total_tweets))
        if archivo_imagen=='':
            plt.show()
        else:
            plt.savefig(archivo_imagen,bbox_inches='tight')
            plt.show()
        plt.clf()
        sbn.reset_orig()
        
    def plot_evolucion_temporal(self, archivo_imagen = '',fecha_filtro = "2021-4-23 18:00:00", frecuencia = '2min'):
        """
        Esta función toma como entrada el archivo de datos preprocesados y 
        devuelve la evolución temporal de cantidad de tweets, RT y QT
        """
        
        d = self.tweets[['id_original', 'fecha_original']].drop_duplicates().rename({'id_original' : 'id', 'fecha_original' : 'fecha'}, axis = 1)
        d['relacion'] = 'Original'
        d = d.append(self.tweets[['id_nuevo','fecha_nuevo','relacion_nuevo_original']].rename({'id_nuevo' : 'id', 'fecha_nuevo' : 'fecha', 'relacion_nuevo_original' : 'relacion'}, axis = 1))
        d['fecha'] = d['fecha'].apply(pd.to_datetime, utc = None)
        d = d[d.fecha > pd.to_datetime(fecha_filtro).tz_localize('utc')]
        d = d.groupby([pd.Grouper(key = 'fecha',
                                  freq = frecuencia),
                       'relacion']).count().reset_index().rename({'relacion' : 'Tipo de Tweet'}, axis = 1)
        sbn.set_context("paper", font_scale = 2)
        fig, ax = plt.subplots(figsize = (11,8))
        sbn.lineplot(x = 'fecha', y = 'id', data = d[d['Tipo de Tweet'] != 'RT'], hue = 'Tipo de Tweet', ax = ax)
        ax_2 = ax.twinx()
        sbn.lineplot(x = 'fecha', y = 'id', data = d[d['Tipo de Tweet'] == 'RT'], label = 'RT', ax = ax_2, color = 'green')
        lines, labels = ax.get_legend_handles_labels()
        lines2, labels2 = ax_2.get_legend_handles_labels()
        ax.legend(loc = 'upper left')
        ax_2.legend(lines + lines2, labels + labels2, loc='upper left')
        ax_2.set_ylabel('Cantidad de RT',)
        ax.set_ylabel('Cantidad de Tweets y QT',)
        ax.set_xlabel('Fecha',)
        ax.set_title('Evolución Temporal',)
        ax.grid(linestyle = 'dashed')
        if archivo_imagen == '':
            plt.show()
        else:
            plt.savefig(archivo_imagen,bbox_inches='tight')
            plt.show()
        plt.clf()            
        sbn.reset_orig()
        

    # ----- Armar grafos para usar en Gephi
    def armar_grafo(self,tipo='usuarios',archivo_grafo='',tipo_enlace='',dirigido=False):
    
        
        '''
        A partir de los datos de retweets preprocesados se arma un grafo con los usuarios como nodos, con los atributos del archivo_atributos
        y los retweets como aristas, pesados por la cantidad de retweets que hubo entre cada enlace, y las posibilidades
        de elegir si en grafo es o no dirigido, y si utilizar los RT, las QT o ambas.
        '''    
        G=nx.Graph()
    
        if tipo=='usuarios':
            # Elegir si el grafo es dirigido o no
            if dirigido:
                G = nx.DiGraph()            
            
            
            datos = self.tweets
            #Elegir si nos quedamos con los enlaces RT, QT (o ambos)    
            if tipo_enlace == 'RT' or tipo_enlace == 'QT':
                datos = self.tweets[self.tweets.relacion_nuevo_original == tipo_enlace]
          
            
            enlace_peso={} #diccionario para poner cada enlace y su peso                      
            for i in range(len(datos)): #Para cada RT y/o QT
                try:
                    enlace_peso [(datos.usuario_nuevo.values[i], datos.usuario_original.values[i])]+=1 #Si ya existe sumar una al enlace
                except:
                    enlace_peso [(datos.usuario_nuevo.values[i], datos.usuario_original.values[i])]=1 #Si no exista agregar el enlace
                G.add_edge(datos.usuario_nuevo.values[i], datos.usuario_original.values[i],relacion=self.tweets.relacion_nuevo_original.values[i]) #Agregar los enclaces
            
            nx.set_edge_attributes(G,enlace_peso,'weight') #Agregar los pesos


            comunidades_louvain = community.best_partition(G)
            
            for us in self.usuarios.keys():
                self.usuarios[us]['Comunidad_Louvain']=comunidades_louvain[us]    
            #Agregar los atributos si está el archivo correspondiente
            nx.set_node_attributes(G,self.usuarios)                
            self.grafo=G
        else:

            if tipo=='menciones':
               texto_usuario_original = self.tweets[['menciones_original','usuario_original']].drop_duplicates().dropna()+self.tweets[['menciones_original','usuario_nuevo']].drop_duplicates().dropna()
               texto_usuario_original = texto_usuario_original.groupby(['usuario_original'])['hashtags_original'].apply(' '.join)        
            elif tipo=='hashtags':
               texto_usuario_original = self.tweets[['hashtags_original','usuario_original']].drop_duplicates().dropna()+self.tweets[['hashtags_original','usuario_nuevo']].drop_duplicates().dropna()
               texto_usuario_original = texto_usuario_original.groupby(['usuario_original'])['hashtags_original'].apply(' '.join)  
            else:
                print('tipo solo puede ser usuarios, menciones o hashtags')
       
            hashtag_ocurrencia = {} #Cantidad de veces que aparece cada hashtag
            enlace_peso = {} #Peso de las aristas (cantidad de tweets que comparten)
            for i in range(len(texto_usuario_original)): #Recorrer las listas de hastgas/menciones de cada tweet
                #Armar para cada tweet una lista con los hashtags/menciones
                try:
                    lista_hashtags_i = sorted(texto_usuario_original.values[i].split(' '))
                    aux = lista_hashtags_i.count('')
                    while aux > 0:
                        lista_hashtags_i.remove('')
                        aux = lista_hashtags_i.count('')
                except:
                    lista_hashtags_i = sorted(texto_usuario_original.values[i].split(' '))
        
                if len(lista_hashtags_i) != 1: #si hay más de uno
                    for j in range(len(lista_hashtags_i)): #Para cada hastag
                        try: #Si ya está en ocurrencia sumar uno
                            hashtag_ocurrencia[saca_tildes(lista_hashtags_i[j].lower())] += 1
                        except: #Si no está agregar
                            hashtag_ocurrencia[saca_tildes(lista_hashtags_i[j].lower())] = 1
                        for k in range(j + 1,len(lista_hashtags_i)): #Recorrer todos los otros hashtags que compartieron ese tweet
                            try: #Si ya estaba la arista sumar uno
                                enlace_peso[(saca_tildes(lista_hashtags_i[j].lower()),saca_tildes(lista_hashtags_i[k].lower()))] += 1
                            except: #Si no estaba agregarla
                                enlace_peso[(saca_tildes(lista_hashtags_i[j].lower()),saca_tildes(lista_hashtags_i[k].lower()))] = 1
                                
            #Agregar todas las aristas con su peso
            for item in enlace_peso.items():
                G.add_edge(item[0][0],item[0][1], weight = item[1])
            
            #Separar en comunidades
            comunidades_louvian = community.best_partition(G)
            
            #Agregar las comunidades y la cantidad de apariciones como atributos
            nx.set_node_attributes(G, comunidades_louvian, 'Comunidad_Louvain')
            nx.set_node_attributes(G, hashtag_ocurrencia, 'Impacto')
            if tipo=='menciones':
                self.grafo_menciones=G
            elif tipo=='hashtags':
                self.grafo_hashtags=G
                
        #Escribir el grafo en un archivo gexf
        if archivo_grafo!='':
            nx.write_gexf(G, archivo_grafo)
        #Devolver el grafo
        return G
    
    def agregar_centrality(self,centrality='grado'):
        '''
        La idea de esta función es recibir un grafo y devolver un archivo con la lista de nodos
        ordenados por su centralidad.
        Como centralidad puede elegirse 'grado','eigenvector' o 'betweenees'.
        '''
        #Elegir la centralidad y calcularla sobre el grafo
        if centrality=='grado':
            centralidad=nx.degree_centrality(self.grafo)
        elif centrality=='eigenvector':
            centralidad=nx.eigenvector_centrality(self.grafo)
        elif centrality=='betweeness':
            centralidad=nx.betweenness_centrality(self.grafo)
        else:
            print('La centralidad solo puede ser grado, eigenvector o betweeness')
            
    
        nx.set_node_attributes(self.grafo, centralidad, 'Centralidad '+ centrality)
    

  

    def plot_nube(self, usuarios = None, archivo_imagen = '',fecha_inicial = "2020-6-9 00:00:00", fecha_final = "2021-6-9 10:00:00"): 
                          
            #Separamos según si son datos de retweets o los tweets de un usuario  
            if usuarios == None:
                datos = self.tweets.copy()
            elif type(usuarios) == str:
                datos = self.tweets[(self.tweets.usuario_nuevo == usuarios) | (self.tweets.usuario_original == usuarios)].copy()
                
            elif type(usuarios) == list:
                datos = self.tweets[(self.tweets.usuario_nuevo.isin(usuarios)) | (self.tweets.usuario_original.isin(usuarios))].copy()
            datos.fecha_original = pd.to_datetime(datos.fecha_original) #Pasar a formato fecha
            datos.fecha_nuevo = pd.to_datetime(datos.fecha_nuevo) #Pasar a formato fecha
            textos_originales = datos[datos.fecha_original.between(pd.to_datetime(fecha_inicial).tz_localize('UTC'), pd.to_datetime(fecha_final).tz_localize('UTC'))].texto_original.drop_duplicates().values #Quedarse con los textos originales en las fechas correctas
            textos_qt = datos[(datos.fecha_nuevo.between(pd.to_datetime(fecha_inicial).tz_localize('UTC'), pd.to_datetime(fecha_final).tz_localize('UTC'))) & (datos.relacion_nuevo_original == 'QT')].texto_nuevo.drop_duplicates().values #Quedarse con los textos de las citas en las fechas correctas
            # No incluimos RT, porque el texto es el mismo que en el original
            textos_completos=np.hstack([textos_originales,textos_qt])
            nube_palabras(textos_completos, archivo_imagen )
    
    def plot_principales_Hashtags(self, archivo_imagen = '', fecha_inicial = '', fecha_final = ''):
        datos = pd.DataFrame(data = {'Hashtags' : ' '.join(self.tweets.hashtags_nuevo.dropna().values).split()})['Hashtags'].value_counts().sort_values(ascending = True).copy()
        
        sbn.set_context("paper", font_scale = 2)
        fig, ax = plt.subplots(figsize = (11,8))
        datos.plot(kind = 'barh', ax = ax)
        ax.barh(y = range(len(datos)),
                width = datos.values,
                color = plt.get_cmap('Set2').colors,
                tick_label = datos.keys())
        ax.grid(linestyle = 'dashed')
        ax.set_xlabel('Cantidad de Apariciones')
        ax.set_title('Hashtags Principales')
        if archivo_imagen == '':
            plt.show()
        else:
            plt.savefig(archivo_imagen,bbox_inches='tight')
            plt.show()            
        sbn.reset_orig()

# -------------- Defino una función de lectura y pre-procesamiento del archivo con los tweets descargados ------------


def guardar_tweet(tweet):
                id_nuevo = tweet['id_str'] # ID identificatorio del tweet
                if 'extended_tweet' in tweet.keys():
                    texto_nuevo = tweet['extended_tweet']['full_text'].replace(',',' ').replace('\n',' ') # texto del tweet nuevo
                else:
                    texto_nuevo = tweet['text'].replace(',',' ').replace('\n',' ')
                usuario_nuevo = tweet['user']['screen_name'] # nombre del usuario que genera el tweet
                fecha_nuevo = pd.to_datetime(tweet['created_at']) - timedelta(hours = 3) #Fecha del Tweet nuevo
                #Incorporamos en hastags_nuevo los hastags con el formato 'hash1 hash2 ...' y  ' si no hay
                hashtags_nuevo = ''
                if len(tweet['entities']['hashtags']) != 0:
                    for h in tweet['entities']['hashtags']:
                        hashtags_nuevo += h['text'] + ' '
                #Incorporamos en menciones_nuevo las menciones con el formato 'men1 men2 ...' y  ' si no hay
                menciones_nuevo = ''
                if len(tweet['entities']['user_mentions']) != 0:
                    for um in tweet['entities']['user_mentions']:
                        menciones_nuevo += um['screen_name'] + ' '
                
                # El tweet original según si es retweet o cita
                
                    
                return(            id_nuevo,
                                   texto_nuevo,
                                   usuario_nuevo,
                                   fecha_nuevo,
                                   hashtags_nuevo,
                                   menciones_nuevo)                    

def guardar_usuario(usuario):
                        dic={}
                        dic['screen_name'] = usuario['screen_name'] #screen name
                        dic['name'] = usuario['name'].replace(',',' ').replace('\n',' ')# nombre del usuario que genera el tweet (sin comas ni enters) 
                        dic['id']=usuario['id'] #id del usuario
                        try: #location (si tiene, sin comas ni enters)
                            dic['location']=usuario['location'].replace(',',' ').replace('\n',' ')
                        except:
                            dic['location']=0
                        try:   #Descripción, si tiene, sin comas, enters ni links y en minúsculas
                            dic['description']=re.sub(r'https?:\/\/\S*', '', usuario['description'].replace(',',' ').replace('\n',' '), flags=re.MULTILINE).lower()
                        except:
                            dic['description']=0
                        #El resto de los atributos
                        dic['verified']=usuario['verified']
                        dic['followers_count']=usuario['followers_count']
                        dic['friends_count']=usuario['friends_count']
                        dic['listed_count']=usuario['listed_count']
                        dic['favourites_count']=usuario['favourites_count']
                        dic['statuses_count']=usuario['statuses_count']
                        dic['created_at']=usuario['created_at']#pd.to_datetime(usuario['created_at']) - timedelta(hours = 3) #formato fecha
                        return(dic)

def procesamiento(archivo_tweets,archivo_guardado,archivo_usuarios):
        usuarios={}
        with open(archivo_guardado, 'w', encoding = 'utf-8') as arch:
                    arch.write('{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format("id_nuevo",
                               "texto_nuevo",
                               "usuario_nuevo",
                               "fecha_nuevo",
                               "hashtags_nuevo",
                               "menciones_nuevo",
                              
                               
                               "id_original",
                               "texto_original",
                               "usuario_original",
                               "fecha_original",
                               "hashtags_original",
                               "menciones_original",

                               "relacion_nuevo_original"))

        with open(archivo_tweets, 'r', buffering = 1000000) as f:
            for line in f.read().split('\n'):
                
                if len(line) != 0:
                    
                    tweet = json.loads(line) # Transformamos la línea (osea el tweet) en un json (diccionario anidado)
                    usuario=tweet['user']
                    usuarios[usuario['screen_name']]=guardar_usuario(usuario)
                    if 'retweeted_status' in tweet.keys(): #Si el original es cita
                        tweet_original = tweet['retweeted_status'] 
                        usuario=tweet_original['user']
                        usuarios[usuario['screen_name']]=guardar_usuario(usuario)

                        relacion_nuevo_original = 'RT' #Guardamos que la relación es un retweet
                        
                        #Guardamos toda la fila
                        with open(archivo_guardado, 'a', encoding = 'utf-8') as arch:
                            t1,t2,t3,t4,t5,t6=guardar_tweet(tweet)
                            arch.write('{},{},{},{},{},{},'.format(t1,t2,t3,t4,t5,t6))
                            t1,t2,t3,t4,t5,t6=guardar_tweet(tweet_original)
                            arch.write('{},{},{},{},{},{},'.format(t1,t2,t3,t4,t5,t6))
                            arch.write('{}\n'.format(relacion_nuevo_original))                    #Citas
                    elif 'quoted_status' in tweet.keys(): #Si el original es una cita
                        tweet_original = tweet['quoted_status']
                        usuario=tweet_original['user']
                        usuarios[usuario['screen_name']]=guardar_usuario(usuario)
  
                        relacion_nuevo_original = 'QT' #Guardamos que la relación es un retweet
    
                        with open(archivo_guardado, 'a', encoding = 'utf-8') as arch:
                            t1,t2,t3,t4,t5,t6=guardar_tweet(tweet)
                            arch.write('{},{},{},{},{},{},'.format(t1,t2,t3,t4,t5,t6))
                            t1,t2,t3,t4,t5,t6=guardar_tweet(tweet_original)
                            arch.write('{},{},{},{},{},{},'.format(t1,t2,t3,t4,t5,t6))
                            arch.write('{}\n'.format(relacion_nuevo_original))
                    else:
                        relacion_nuevo_original = 'Original' #Guardamos que la relación es un retweet
                        with open(archivo_guardado, 'a', encoding = 'utf-8') as arch:
                            t1,t2,t3,t4,t5,t6=guardar_tweet(tweet)
                            arch.write('{},{},{},{},{},{},'.format(t1,t2,t3,t4,t5,t6))
                            arch.write('{},{},{},{},{},{},'.format("","","","","",""))
                            arch.write('{}\n'.format(relacion_nuevo_original))   
                    
                    with open(archivo_usuarios, 'w') as outfile:
                        json.dump(usuarios, outfile)            
                    


                

# --------- Defino función auxiliar para ordenar comunidades----------------
def ordeno_comunidades(diccionario):
    '''
    Esta es una función auxiliar para ordenar las comunas por tamaño, utilizada en las nubes de palabras
    '''
    
    lista_com = list(diccionario.values()) 
    lista_com_ordenadas = [] #Acá vamos a agregar a las comunas sus tamaños
    for c in set(lista_com): #Armar la nueva lista
        lista_com_ordenadas.append([lista_com.count(c), c]) 
    lista_com_ordenadas = sorted(lista_com_ordenadas, reverse = True) #Ordenarla por tamaño
    lista_com_ordenadas = [lista_com_ordenadas[i][1] for i in range(len(lista_com_ordenadas))] #Sacarle el tamaño
    return lista_com_ordenadas


#---------- Defino función que permite generar nubes de palabras en función de la comuna a la que correspondan-------------

def nube_de_palabras_por_comuna_textos(archivo_procesado, archivo_grafo, archivo_imagen,n_comunas = 5):
    """
    Le paso el archivo procesado y le paso
    el archivo de grafo (es el que tiene la información sobre las comunas)
    
    La idea es que devuelva una nube de palabras para cada una de las principales n comunas
    """
    #Levantar los datos del archivo
    datos = pd.read_csv(archivo_procesado,
                            sep = ',',
                            error_bad_lines = False)
    
    #Levantar el grafo y las comunidades
    grafo = nx.read_gexf(archivo_grafo)
    comunidades = dict(nx.get_node_attributes(grafo,'Comunidad_Louvain'))
    print(comunidades)
    del grafo #Elimino el grafo para que no ocupe espacio
    
    #Incorporo a los datos los valores de las comunas del usuario del tweet nuevo y del original    
    datos['Comuna_Nuevo'] = [comunidades[usuario] if usuario in comunidades.keys() else None for usuario in datos.usuario_nuevo.values]
    datos['Comuna_Original'] = [comunidades[usuario] if usuario in comunidades.keys() else None for usuario in datos.usuario_original.values]    
    
    comunidades_ordenadas = ordeno_comunidades(comunidades) #Ordeno las comunidades

    for c in comunidades_ordenadas[:n_comunas]:  #para las n primeras comunidades (las más grandes, porque está ordenado)     
        textos_originales = datos[datos.Comuna_Original == c].texto_original.drop_duplicates().values #Los textos de esas comunas originales
        textos_qt = datos[datos.Comuna_Nuevo == c][datos.relacion_nuevo_original == 'QT'].texto_nuevo.drop_duplicates().values #Las citas de esas comunas
        # No incluimos RT, porque el texto es el mismo que en el original

        textos = [] #Juntamos todos los textos (con minúsculas y sin links)
        for t in textos_originales:
            textos.append(re.sub(r'https?:\/\/\S*', '', t, flags=re.MULTILINE).lower())
        for t in textos_qt:
            textos.append(re.sub(r'https?:\/\/\S*', '', t, flags=re.MULTILINE).lower())

        #Filtramos las stopwords
        stopwords_file = 'stopwords_spanish_modificado.txt'
        textos = ''.join(textos).replace(',',' ').replace('.',' ').replace("'",' ').split(' ')
        stopwords = codecs.open(stopwords_file,'r','utf8').read().split('\r\n')
        textos_filtrado = list(filter(lambda x: x not in stopwords, textos))
        textos = ' '.join(textos_filtrado)
        textos = saca_tildes(textos)
        wc = WordCloud(width=1600,
                       height=800,
                       background_color = "white",
                       contour_width = 3,
                       contour_color = 'steelblue',
                       max_words = 100,
                       collocations=False).generate_from_text(textos)
        plt.figure(figsize = (10,8), dpi = 100)
        plt.title('Nube de Palabras - Comuna {}'.format(c), fontsize = 20)
        plt.imshow(wc, interpolation='bilinear')
        plt.axis("off")
        plt.savefig(archivo_imagen[:-4] + 'Comuna{}'.format(c) + archivo_imagen[-4:],bbox_inches='tight')
    #    plt.show()
        plt.clf()
    
def nube_de_palabras_por_comuna_descripciones(archivo_procesado, archivo_grafo, archivo_imagen,n_comunas = 5):
    """
    Le paso el archivo procesado de usuarios y le paso
    el archivo de grafo (es el que tiene la información sobre las comunas)
    
    La idea es que devuelva una nube de palabras para cada una de las principales n comunas
    """
    
    #Levantar los datos del archivo
    datos = pd.read_csv(archivo_procesado,
                        sep = ',',
                        error_bad_lines = False,
                        #encoding = 'latin-1',
                        encoding='utf8',
                        lineterminator='\n')
    
    #Levantar el grafo y las comunidades
    grafo = nx.read_gexf(archivo_grafo)
    comunidades = dict(nx.get_node_attributes(grafo,'Comunidad_Louvain'))
    del grafo #Elimino el grafo para que no ocupe espacio
    
    #Incorporo a los datos los valores de las comunas del usuario del tweet nuevo y del original    
    datos['Comuna'] = [comunidades[usuario] if usuario in comunidades.keys() else None for usuario in datos.screen_name.values]
    comunidades_ordenadas = ordeno_comunidades(comunidades) #Ordeno las comunidades
    
    for c in comunidades_ordenadas[:n_comunas]:   #para las n primeras comunidades (las más grandes, porque está ordenado)          
        textos_descripciones = datos[datos.Comuna == c].description.drop_duplicates().values
        textos = [] #Juntamos todos los textos (con minúsculas y sin links)
        for t in textos_descripciones:
            try:
                textos.append(re.sub(r'https?:\/\/\S*', '', t, flags=re.MULTILINE).lower())
            except:
                pass
            
        #Filtramos las stopwords
        stopwords_file = 'stopwords_spanish_modificado.txt'
        textos = ''.join(textos).replace(',',' ').replace('.',' ').replace("'",' ').split(' ')
        stopwords = codecs.open(stopwords_file,'r','utf8').read().split('\r\n')
        textos_filtrado = list(filter(lambda x: x not in stopwords, textos))
        textos = ' '.join(textos_filtrado)
        textos = saca_tildes(textos)

        #Acomodamos los emoticones para que se vean        
        normal_word = r"(?:\w[\w']+)"
        ascii_art = r"(?:[{punctuation}][{punctuation}]+)".format(punctuation=string.punctuation)
        emoji = r"(?:[^\s])(?<![\w{ascii_printable}])".format(ascii_printable=string.printable)
        regexp = r"{normal_word}|{ascii_art}|{emoji}".format(normal_word=normal_word, ascii_art=ascii_art,                                                     emoji=emoji)
        d = path.dirname(__file__) if "__file__" in locals() else os.getcwd()
        font_path = path.join(d, 'Symbola.ttf')
        
        #Armamos la nube
        wc = WordCloud( font_path=font_path,regexp=regexp,
                       width=1600,
                       height=800,
                       background_color = "white",
                       contour_width = 3,
                       contour_color = 'steelblue',
                       max_words = 100,
                       collocations=False).generate_from_text(textos)
        plt.figure(figsize = (10,8), dpi = 100)
        plt.title('Nube de Palabras - Comuna {}'.format(c), fontsize = 20)
        plt.imshow(wc, interpolation='bilinear')
        plt.axis("off")
        plt.savefig(archivo_imagen[:-4] + 'Comuna{}'.format(c) + archivo_imagen[-4:],bbox_inches='tight')
        #    plt.show()
        plt.clf()
    
    