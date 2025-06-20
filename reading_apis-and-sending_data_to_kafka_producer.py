from kafka import KafkaProducer
import json
import time
from datetime import datetime
import os
import requests
import base64
from datetime import datetime

# Configuration
KAFKA_BROKER = 'kafka:9092' 
TOPIC_NAME = 'transactions'
CLIENT_ID = "2a79b712f5ff406c87eb839278c475e6"
CLIENT_SECRET = "847213cf05de4367bf4e4ede8539d51a"  
BASE_DIR = "api_data"
WEATHER_API_FOLDER = f"{BASE_DIR}/weather"
CRYPTO_API_FOLDER = f"{BASE_DIR}/crypto"
ECOMMERCE_API_FOLDER = f"{BASE_DIR}/ecommerce"
SPOTIFY_ALBUMS_FOLDER = f"{BASE_DIR}/spotify_album"
SPOTIFY_ARTIST_FOLDER = f"{BASE_DIR}/spotify_artist"
SPOTIFY_TRACK_FOLDER = f"{BASE_DIR}/spotify_track"

num_messages=5
os.makedirs(BASE_DIR, exist_ok=True)

def get_weather(city="London"):
    print("Executing weather api function")
    url = f"https://api.open-meteo.com/v1/forecast?latitude=35&longitude=139&hourly=temperature_2m"
    response = requests.get(url)
    return response.json()
  

def get_crypto_data(coin="bitcoin"):
    print("Executing crypto api function")
    url = f"https://api.coingecko.com/api/v3/coins/{coin}"
    response = requests.get(url)
    return response.json()

def get_ecommerce_data():
    # Example endpoint - replace with actual API
    print("Executing ecommerce api function")
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)
    return response.json()

def save_data(folder_name, data, prefix="data"):
    #print(f"writing {folder_name} api data to file ")
    folder_path = os.path.join(BASE_DIR, folder_name)
    print(f"inside save_data function: folder_path {folder_path}")
    os.makedirs(folder_path, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.json"
    filepath = os.path.join(folder_path, filename)
    print(f"filepath: {filepath}")
    print(f"folder_name: {folder_name}")
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)
    #print(f"Saved {filename} in {folder_name}")
    topicName = folder_name    
    try:
         topic_to_folder = {
        "weather": WEATHER_API_FOLDER,
        "crypto": CRYPTO_API_FOLDER,
        "ecommerce": ECOMMERCE_API_FOLDER,
        "spotify_album": SPOTIFY_ALBUMS_FOLDER,
        "spotify_artist": SPOTIFY_ARTIST_FOLDER,
        "spotify_track": SPOTIFY_TRACK_FOLDER,
       }

         filepath = topic_to_folder.get(topicName)
    
         if filepath:
             produce_json_files(filepath, topicName, bootstrap_servers=KAFKA_BROKER)
         else:
             print("No valid topic mentioned.")
        
    except FileNotFoundError:
        raise FileNotFoundError(f"The file does not exist: {filepath}")

        


     
def get_spotify_data(access_token, endpoint, entity_id):
    print("Executing spotify api function")
    base_url = "https://api.spotify.com/v1/"
    api_headers = {"Authorization":f"Bearer {access_token}"}
    response = requests.get(f"{base_url}{endpoint}/{entity_id}", headers=api_headers)
    data = response.json()
    #print(f" data: {data}")
    return data

def get_spotify_token(client_id, client_secret ):
    # getting token
    print("getting spotify access token")
    credentials = f"{client_id}:{client_secret}".encode("latin-1")
    encoded_credentials = base64.b64encode(credentials).decode("latin-1")         
    token_url = "https://accounts.spotify.com/api/token"            
    headers = {
           "Authorization" : f"Basic {encoded_credentials}",
           "Content-Type": "application/x-www-form-urlencoded"
            }       
    data = {"grant_type" : "client_credentials"}
    try:
        token_response = requests.post(token_url, headers=headers, data=data, timeout=10)
        #token_response.raise_for_status()
        access_token = token_response.json().get("access_token")
    except requests.exceptions.RequestException as e :
        print(f" api check failed:  {e}")
    return access_token
    
def fetch_spotify_info():
  
    access_token = get_spotify_token(CLIENT_ID,CLIENT_SECRET )
    if not access_token:
           print("Failed to get access token, accessing public api")
    print(f"access token = {access_token}")
    
    album_data = get_spotify_data(access_token, "albums", "4aawyAB9vmqN3uQ7FjRGTy")
    save_data("spotify_album", album_data, "album")
    
    artist_data = get_spotify_data(access_token, "artists", "0TnOYISbd1XYRBk9myaseg")
    save_data("spotify_artist", artist_data, "artist")
    
    track_data = get_spotify_data(access_token, "tracks", "11dFghVXANMlKmJXsNCbNl")
    save_data("spotify_track", track_data, "track")

def create_producer():
    print("Executing create_producer function")
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        linger_ms=10,
        compression_type='gzip'
    )

def generate_sample_transaction():
    
    """Generate realistic transaction data"""
    users = ['alice', 'bob', 'charlie', 'diana', 'eve']
    actions = ['purchase', 'refund', 'add_to_cart', 'view_item']
    
    return {
        'user_id': users[int(time.time()) % len(users)],
        'action': actions[int(time.time()) % len(actions)],
        'amount': round((time.time() % 1000) / 10, 2),
        'timestamp': datetime.utcnow().isoformat(),
        'device': ['mobile', 'desktop'][int(time.time()) % 2]
    }

def produce_messages(producer, num_messages=10):
    print("Executing produce_messages function")
    for _ in range(num_messages):
        try:
            transaction = generate_sample_transaction()
            print("Executing generate_sample_transaction function")
            future = producer.send(TOPIC_NAME, value=transaction)
            
            # Optional: Verify message delivery
            metadata = future.get(timeout=10)
            print(f"Sent to {metadata.topic}[{metadata.partition}] @ offset {metadata.offset}")
            
            time.sleep(0.5)  # Throttle message rate
            
        except Exception as e:
            print(f"Error sending message: {e}")

def produce_json_files(filepath, topicName, bootstrap_servers=KAFKA_BROKER):
    print("Executing produce_json_files function")
    print("----------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>----------------------")
    print(f"filepath:  {filepath}")
    print(f"topicName:  {topicName}")       

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Process each JSON file in the folder
    for filename in os.listdir(filepath):
        if filename.endswith('.json'):
            file_path = os.path.join(filepath, filename)
            
            try:
                with open(file_path, 'r') as file:
                    # Load JSON data
                    json_data = json.load(file)
                    
                    # Send to Kafka
                    producer.send(topicName, value=json_data)
                    print(f"Sent {filename} to topic {topicName}")
                    
            except json.JSONDecodeError:
                print(f"Invalid JSON format in file: {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
    
    # Flush and close producer
    producer.flush()
    producer.close()
    print("Finished processing all files")

def safe_api_call(api_func, save_name, *args, **kwargs):
    try:
        data = api_func(*args, **kwargs)
        save_data(save_name, data)
        return True
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {save_name}: {str(e)}")
    except ValueError as e:
        print(f"Data format issue for {save_name}: {str(e)}")
    except Exception as e:
        print(f"Unexpected error in {save_name}: {str(e)}")
    return False



    
def api_data_generate_code():
    
    # fetching spotify data
    fetch_spotify_info()
    
    # fetching PUBLIC APIS DATA
    # # Fetch and save weather data
    # weather_data = get_weather("London")
    # save_data("weather", weather_data)
    
    safe_api_call(get_weather, "weather", "London")
    
    #Fetch and save crypto data
    # crypto_data = get_crypto_data()
    # save_data("crypto", crypto_data)
    
    safe_api_call(get_crypto_data, "crypto")
    
    # Fetch and save ecommerce data
    # ecommerce_data = get_ecommerce_data()
    # save_data("ecommerce", ecommerce_data)

    safe_api_call(get_crypto_data, "ecommerce")
    
    
def producer_code(): 
    
    print("Starting Kafka Producer...")
    producer = create_producer()
    
    try:
        produce_messages(producer,num_messages)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed")
    


def main():    
           
    api_data_generate_code()
    producer_code()
         
    
if __name__ == "__main__":
    main()
