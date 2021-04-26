#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 23 18:23:39 2021

@author: lucia
"""
archivo_tweets='PruebaLarreta_Tweets.txt'
archivo_guardado='PruebaLarreta_procesado.txt'
archivo_usuarios='PruebaLarreta_usuarios.json'
archivo_grafo='PruebaLarreta_grafo.gexf'
archivo_grafo_hash='PruebaLarreta_grafo_hash.gexf'

procesamiento(archivo_tweets,archivo_guardado ,archivo_usuarios)
Larreta=Bases_Datos()
Larreta.cargar_datos(archivo_guardado)
Larreta.cargar_usuarios(archivo_usuarios)
Larreta.plot_tipo_tweet()
Larreta.plot_evolucion_temporal()
Larreta.armar_grafo(tipo='usuarios',archivo_grafo=archivo_grafo,tipo_enlace='',dirigido=False)
Larreta.armar_grafo(tipo='hashtags',archivo_grafo=archivo_grafo_hash,tipo_enlace='',dirigido=False)
Larreta.agregar_centrality()
Larreta.plot_nube()
Larreta.plot_nube_usuario('veronik50799452')


#En cualquier momento se pueden acceder a las bases con
Larreta.tweets
Larreta.usuarios
Larreta.grafo