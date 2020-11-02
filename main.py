#importaciones
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timezone

#Datos del dashBoard de spootify
clienteID = 'a9b872e33ca84e968f873ed25f8ec058'
clienteIDSecreto = '44dd2e5add92443db3dd0960a5772024'
#datos del url de spootify
url = 'https://accounts.spotify.com/api/token'
urlBase = 'https://api.spotify.com/v1/'
#peticion de acceso
resp = requests.post(url, { 'grant_type': 'client_credentials', 'client_id': clienteID, 'client_secret': clienteIDSecreto })

#
if resp.status_code == 200:
    #creacion de json
    respJson = resp.json()
    #creacion de token de acceso
    TokenDeAcceso = respJson["access_token"]
    #autorizacion de acceso
    headers = {"Authorization": "Bearer {token}".format(token=TokenDeAcceso)}

    #id de los artistas
    idBlackSabbath = '5M52tdBnJaKSvOpJGz8mfZ'
    idBodDylan = '74ASZWbe4lXaubB36ztrGX'
    idGrupoNiche = '1zng9JZpblpk48IPceRWs8'
    idLedZeppelin = '36QJpDe2go2KgaRleHCDTp'
    idMetallica = '2ye2Wgw4gimLv2eAKyk1NB'
    idMichaelJackson = '3fMbdgg4jU18AjLCKBhRSm'

    #Arreglos en los que se guarda los datos del artista
    artistasID = [idBlackSabbath, idBodDylan, idGrupoNiche, idLedZeppelin, idMetallica, idMichaelJackson]
    nombresArtistas = []
    popularidadArtistas = []
    tiposArtistas = []
    urisArtistas = []
    seguidoresArtistas = []
    origenesArtistas = []
    fechasCargaArtistas = []

    #Arreglos en los que se guarda los datos de las canciones
    CancionesID = []
    nombresCanciones = []
    tiposCanciones = []
    artistasCanciones = []
    albumCanciones = []
    numerosCanciones = []
    popularidadCanciones = []
    urisCanciones = []
    fechaDeLanzamientoCanciones = []
    generosCanciones = []
    origenCanciones = []
    fechaDeCargaCanciones = []

    for artistaID in artistasID:
        #solicitud para obtner la informacion del artista
        datos = requests.get(urlBase + 'artists/' + artistaID, headers=headers, )
        datosArtista = datos.json()
        #guardar los datos de los artistas en el arreglo
        nombresArtistas.append(datosArtista["name"])
        popularidadArtistas.append(datosArtista['popularity'])
        tiposArtistas.append(datosArtista['type'])
        urisArtistas.append(datosArtista['uri'])
        seguidoresArtistas.append(datosArtista['followers']['total'])
        fechaActual = datetime.now()
        fechasCargaArtistas.append(fechaActual.replace(tzinfo=timezone.utc).timestamp())
        origenesArtistas.append(datosArtista['href'])

        # solicitud para obter la informacion de las canciones del artista
        respuesta = requests.get(urlBase + 'artists/' + artistaID + '/top-tracks?market=ES', headers=headers, params={'include_groups': 'album', 'limit': 15})
        datosCanciones = respuesta.json()["tracks"]
        # guardar los datos de las canciones del artistas en el arreglo
        for datosCancion in datosCanciones:
            nombresCanciones.append(datosCancion['name'])
            tiposCanciones.append(datosCancion['type'])
            popularidadCanciones.append(datosCancion['popularity'])
            artistasCanciones.append(datosCancion['artists'][0]["name"])
            albumCanciones.append(datosCancion['album']['name'])
            numerosCanciones.append(datosCancion['disc_number'])
            CancionesID.append(datosCancion['id'])
            urisCanciones.append(datosCancion['uri'])
            fechaDeLanzamientoCanciones.append(datosCancion['album']['release_date'])
            generosCanciones.append(datosArtista['genres'])
            fechaActual = datetime.now()
            fechaDeCargaCanciones.append(fechaActual.replace(tzinfo=timezone.utc).timestamp())
            origenCanciones.append(datosCancion['href'])

#se crea tabla de los artistas
tablaArtistas = {
                    'Nombre del artista': nombresArtistas,
                    'Popularidad': popularidadArtistas,
                    'Tipo': tiposArtistas,
                    'Uri': urisArtistas,
                    'Cantidad de followers': seguidoresArtistas,
                    'Origen': origenesArtistas,
                    'FechaCarga': fechasCargaArtistas
                }

#se crea tabla de las canciones
tablaCanciones ={
                    'ID': CancionesID,
                    'Nombre del track': nombresCanciones,
                    'Tipo de track': tiposCanciones,
                    'Artista': artistasCanciones,
                    'Album': albumCanciones,
                    'Track number': numerosCanciones,
                    'Popularidad': popularidadCanciones,
                    'Uri': urisCanciones,
                    'Fecha de lanzamiento': fechaDeLanzamientoCanciones,
                    'Géneros': generosCanciones,
                    'Origen': origenCanciones,
                    'FechaCarga': fechaDeCargaCanciones
                }

#dar formato a los datos
artistas = pd.DataFrame(data=tablaArtistas)
canciones = pd.DataFrame(data=tablaCanciones)
#cominucacion con la base de datos
motor = create_engine('postgresql://postgres:12345@127.0.0.1:5432/postgres')
#cargar los datos en la base de datos
artistas.to_sql('Artistas', con=motor, index=False)
canciones.to_sql('Tracks', con=motor, index=False)