import socket
import json
import threading
import time
import uuid
import sys
import random
import os
import select
import hashlib
from datetime import datetime



####################################################################
################### Database and Meta Data Storage #################
####################################################################
# Directory and file for persistent metadata storage
DB_DIR = "db"
METADATA_FILE = os.path.join(DB_DIR, "metadata.json")
FILE_LIST = {}  # Global dictionary to store file metadata
LOCAL_FILES_PATH = os.path.join(DB_DIR, "local_files.json")



def initialize_database():
    """
    Ensure that the database directory and metadata file exist.
    If the metadata file does not exist, create an empty JSON file.
    """
    os.makedirs(DB_DIR, exist_ok=True)
    if not os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "w") as f:
            json.dump({}, f)


def load_local_files():
    local_files_data = None
    with open(LOCAL_FILES_PATH, "r") as f:
        local_files_data = json.load(f)  # Store the data in the variable

    return local_files_data  # Return the stored data


def generate_file_id(filename, timestamp):
    """
    Generate a unique file ID using the file's content and a provided timestamp.
    """
    # Get the file content (you'll need to read the file to get its content)
    full_path = os.path.join(DB_DIR, filename)  

    with open(full_path, 'rb') as f:
        content = f.read()

    # Hash the content and the provided timestamp (not the current time)
    hashBase = hashlib.sha256()
    hashBase.update(content)
    hashBase.update(str(timestamp).encode())  # Use the provided timestamp

    # Return the hexadecimal hash
    return hashBase.hexdigest()


def add_file_metadata(filename, peer_id, has_copy,timestamp=None):
    """
    Add a new file's metadata to the FILE_LIST, including the generated file_id.
    If no timestamp is provided, use the current time as fallback.
    """
    if timestamp is None:
        timestamp = int(time.time())  # Use the current time if no timestamp is provided

    full_path = os.path.join(DB_DIR, filename)  
    file_id = generate_file_id(filename, timestamp)  # Generate file_id using the provided timestamp
    file_size = os.path.getsize(full_path)/(1024*1024)

   

    FILE_LIST[file_id] = {
        "file_name": filename,
        "file_size": file_size,
        "file_id": file_id,
        "file_owner": peer_id,
        "file_timestamp": timestamp,  # Store the original timestamp
        "peers_with_file": [],  # Track which peers have the file
        "has_copy": has_copy
    }
    return file_id


# A threading lock to protect concurrent access to the local files
local_lock = threading.Lock()
def save_local_files():
    """
    Save the current local file metadata to LOCAL_FILES_PATH.
    This function ensures the file list is locked while saving.
    """
    with local_lock:
        # Lock the access to FILE_LIST to prevent other threads from modifying it while saving.
        
        # Convert FILE_LIST to a local_files structure if needed
        local_files = {
            file_id: {
                "file_name": file_info["file_name"],
                "file_size": file_info["file_size"],
                "file_id": file_info["file_id"],
                "file_owner": file_info["file_owner"],
                "file_timestamp": file_info["file_timestamp"]
            }
            for file_id, file_info in FILE_LIST.items()
            if file_info.get("has_copy") == "yes"  # only save local copies
        }

    # Write the local files data to the disk (local_files.json)
    with open(LOCAL_FILES_PATH, "w") as f:
        json.dump(local_files, f, indent=4)

write_lock = threading.Lock()
def write_local_files_data(local_files_data):
    with write_lock:
        with open(LOCAL_FILES_PATH, "w") as f:
            json.dump(local_files_data, f, indent=4)


def delete_local_file(file_id):
    """Delete file from local storage if exists, and update local metadata."""
    with local_lock:
        local_files_data = load_local_files()

        if file_id in local_files_data:
            file_data = local_files_data[file_id]
            file_name = file_data.get("file_name")
            file_path = os.path.join(DB_DIR, file_name)

            try:
                # Remove the file from local storage (disk)
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Deleted file {file_name} from local storage.")
                
                # Remove the file from the local metadata
                del local_files_data[file_id]
                # Save the updated local file metadata to disk
                write_local_files_data(local_files_data)

                # Update FILE_LIST to mark has_copy as "no"  <-- ADD THIS
                if file_id in FILE_LIST:
                    FILE_LIST[file_id]["has_copy"] = "no"  # <-- KEY FIX
                    
            except Exception as e:
                print(f"[ERROR] Failed to delete file {file_name} from local storage: {str(e)}")
        else:
            print(f"File with ID {file_id} not found in local files.")

# A threading lock to protect concurrent access to the metadata file
metadata_lock = threading.Lock()

def update_metadata_file(new_data):
    """
    Thread-safe update of metadata.json.
    This function merges new_data into the global FILE_LIST and writes the updated
    FILE_LIST to the metadata file.
    """
    global FILE_LIST
    with metadata_lock:
        FILE_LIST.update(new_data)  # Merge new data into FILE_LIST
        with open(METADATA_FILE, "w") as f:
            json.dump(FILE_LIST, f, indent=4)

####################################################################
####################################################################
####################################################################




####################################################################
########################### Configuration ##########################
####################################################################
errorMess = """Please enter valid commands:
LOGIN <username> (only once)
PUSH <FILENAME> (in your directory)
LIST - lists all files in the server
GET <FILENAME> (in the server)
DELETE <FILENAME> (in the server)
cd <directory> - Change local directory
ls - List local files
"""

LOCAL_PORT = None
UMNETID = ""
LOCAL_HOST = ""  # Set to your IP if testing across machines
WEB_PORT = None

# Constants for gossiping and peer management
GOSSIP_INTERVAL = 30    # Seconds between gossip messages
PEER_TIMEOUT = 60       # Seconds before a peer is considered dead
CLEANUP_INTERVAL = 10   # Seconds between cleaning up dead peers
MAX_FORWARD_PEERS = 2   # Maximum number of peers to forward a gossip message to
GOSSIP_WAIT_TIME = 3

####################################################################
####################################################################
####################################################################




####################################################################
####################### Peer Resolution Helpers ####################
####################################################################
def resolve_hostname(hostname):
    """
    Resolve a hostname (e.g. eagle.cs.umanitoba.ca) to its IP address.
    Returns the IP address as a string, or None if resolution fails.
    """
    try:
        ip = socket.gethostbyname(hostname)
        return ip
    except socket.gaierror:
        return None

def resolve_hosts(hosts):
    """
    Given a list of (hostname, port) tuples, resolve the hostnames to IP addresses.
    Returns a list of (ip, port) tuples.
    """
    resolved = []
    for host, port in hosts:
        ip = resolve_hostname(host)
        if ip:
            resolved.append((ip, port))
        else:
            print(f"Warning: Could not resolve {host}")
    return resolved

def is_ip_address(host):
    """
    Check if the given host string is a valid IPv4 address.
    """
    try:
        socket.inet_pton(socket.AF_INET, host)
        return True
    except socket.error:
        return False

def get_normalized_peer_map():
    """
    Returns a dict of peerId → (ip, port) using resolved IPs and most recent last_seen.
    """
    normalized = {}

    for (host, port), info in TRACKED_PEERS.items():
        ip = resolve_hostname(host) if not host.replace('.', '').isdigit() else host  # Only resolve if it's not already an IP
        if not ip:
            print(f"[WARN] Could not resolve {host}, skipping...")
            continue
        
        peerId = info["peerId"]
        if peerId not in normalized or info["last_seen"] > TRACKED_PEERS[normalized[peerId]]["last_seen"]:
            normalized[peerId] = ((ip, port))

    return normalized

# Define well-known bootstrap peers by hostname, then resolve them to IP addresses.
WELL_KNOWN_HOSTS_RAW = [("hawk.cs.umanitoba.ca", 8999), ("silicon.cs.umanitoba.ca", 8999), ("eagle.cs.umanitoba.ca",8999),("grebe.cs.umanitoba.ca", 8999)]
WELL_KNOWN_HOSTS = resolve_hosts(WELL_KNOWN_HOSTS_RAW)

####################################################################
####################################################################
####################################################################


# Dictionary to track peers: key=(ip, port), value=last seen timestamp
TRACKED_PEERS = {}

SEEN_GOSSIP_IDS = set()
SENT_GOSSIPS = set()
####################################################################
########################### Messaging Functions ####################
####################################################################


def send_message(host, port, data):
    """
    Create a TCP connection to the given host and port and send the provided data.
    Returns True on success, False on failure.
    """
    # Resolve the host
    resolved_host = resolve_hostname(host)
    if not resolved_host:
        print(f"Failed to resolve host: {host}")
        return False  # Return false if the resolution failed

    try:
        # Create the socket with a connection timeout
        with socket.create_connection((resolved_host, port), timeout=5) as sock:
            sock.settimeout(5)  # Set timeout for sending/receiving
            sock.sendall(data)
        #print(f"Message successfully sent to {resolved_host}:{port}")
        return True 
    except socket.timeout:
        pass
    except ConnectionRefusedError:
        pass
    except Exception as e:
        pass
    return False



def send_gossip():
    """
    Generate and send a gossip message to one well-known host and then forward it to random peers.
    """
    gossip_id = str(uuid.uuid4())
    message = {
        "type": "GOSSIP",
        "host": LOCAL_HOST,
        "port": LOCAL_PORT,
        "id": gossip_id,
        "peerId": UMNETID
    }  

    SENT_GOSSIPS.add(gossip_id)

    data = json.dumps(message).encode()

    # Choose a random well-known host to send the gossip to.
    if WELL_KNOWN_HOSTS:
        chosen_host, chosen_port = random.choice(WELL_KNOWN_HOSTS)
        resolved_host = resolve_hostname(chosen_host)
        if resolved_host:
            send_message(resolved_host, chosen_port, data)
            print(f"Gossip sent to {resolved_host}:{chosen_port}")
        else:
            print(f"Failed to resolve well-known host: {chosen_host}")


    # Also forward to random peers (excluding well-known hosts).
    forward_gossip_to_peers(gossip_id, message)



def forward_gossip_to_peers(gossip_id, message, sender_peer=None):
    """
    Forward a gossip message to a random set of peers, excluding well-known hosts and the sender.
    
    Parameters:
      - gossip_id: The unique ID of the gossip message.
      - message: The gossip message dictionary.
      - sender_peer: A tuple (ip, port) of the originator to exclude.
    """
    # Build a list of active peers excluding well-known hosts and the sender.
    active_peers = [p for p in TRACKED_PEERS.keys() 
                    if p not in WELL_KNOWN_HOSTS and p != sender_peer]
    
    # Choose a random subset of up to 5 peers (or fewer if not enough exist).
    num_peers_to_forward = min(5, len(active_peers))
    if active_peers and num_peers_to_forward > 0:
        forward_to = random.sample(active_peers, num_peers_to_forward)
        for peer in forward_to:
            peer_host, peer_port = peer
            resolved_peer_host = resolve_hostname(peer_host)
            if resolved_peer_host:
                send_message(resolved_peer_host, peer_port, json.dumps(message).encode())



def handle_gossip(message, sender_addr):
    """
    Process an incoming gossip message: update peer info, forward it to others,
    and send a GOSSIP_REPLY back to the sender.
    """
    gossip_id = message["id"]
    host =message["host"]
    port = message["port"]

    if not is_ip_address(host):
        resolved_host = resolve_hostname(host)
        if not resolved_host:
            print(f"Could not resolve host {host} from gossip message, skipping...")
            return
        host = resolved_host

    peer_key = (host, port)
    
    if gossip_id in SENT_GOSSIPS:
        return

    SENT_GOSSIPS.add(gossip_id)
   
    # Update last seen time for this peer.
    TRACKED_PEERS[peer_key] = {
        "last_seen": time.time(),
        "peerId": message.get("peerId", "UnknownPeer")
    }

    # Forward this gossip message to other peers (exclude the originator).
    forward_gossip_to_peers(gossip_id, message, sender_peer=peer_key)

    local_files = load_local_files()
   
    local_files_list = [
        {
            "file_name": file_info["file_name"],
            "file_size": file_info["file_size"],
            "file_id": file_id,
            "file_owner": file_info["file_owner"],
            "file_timestamp": file_info["file_timestamp"]
        }
        for file_id, file_info in local_files.items()
    ]

    reply = {
        "type": "GOSSIP_REPLY",
        "host": LOCAL_HOST,
        "port": LOCAL_PORT,
        "peerId": UMNETID,
        "files": local_files_list
    }

    resolved_host = resolve_hostname(message["host"])
    
    if resolved_host:
        send_message(resolved_host, message["port"], json.dumps(reply).encode())
    else:
        print(f"Failed to resolve sender host: {message['host']}")




def parse_gossip_reply(message, sender_addr):
    """Parse incoming GOSSIP_REPLY messages and update metadata."""

    host = message["host"]
    if not is_ip_address(host):
        resolved_ip = resolve_hostname(host)
        if not resolved_ip:
            print(f"Failed to resolve host: {host}")
            return
        host = resolved_ip

    peer_key = (host, message["port"])


    if peer_key == (LOCAL_HOST, LOCAL_PORT):
        return  # Don't track yourself or well-known hosts

    TRACKED_PEERS[peer_key] = {
        "last_seen": time.time(),
        "peerId": message.get("peerId", "UnknownPeer")
    }

    peer_id = message.get("peerId", "UnknownPeer")
    files = message.get('files', [])

    local_files_data = load_local_files()
    new_file_data = {}  # Dictionary to accumulate updates
    try:
        for file_info in files:
            file_id = file_info.get("file_id")
            if not file_id:
                continue

            # Parse the timestamp from the incoming metadata (as a string)
            new_ts_str = file_info.get("file_timestamp")
            
            # Check if this file already exists in our FILE_LIST
            existing_file = FILE_LIST.get(file_id)
            update_needed = False

            if existing_file:
                old_ts_str = existing_file.get("file_timestamp")

            else:
                # If file not in our list, add it.
                update_needed = True

            # Decide on 'has_copy' based solely on whether we have retrieved the file locally.
            has_copy = "yes" if file_id in local_files_data else "no"
            # Ensure we add the current peer to the peers list
            peers_with_file = FILE_LIST.get(file_id, {}).get("peers_with_file", [])
            if peer_id != UMNETID and peer_id not in peers_with_file:
                peers_with_file.append(peer_id)

            if update_needed:
                new_file_data[file_id] = {
                    "file_name": file_info.get("file_name"),
                    "file_size": file_info.get("file_size"),
                    "file_id": file_id,
                    "file_owner": file_info.get("file_owner"),
                    "file_timestamp": new_ts_str,  # Keep the string format
                    "has_copy": has_copy,
                    "peers_with_file": peers_with_file
                }
            else:
                # If no update is needed, we can also update the peers list.
                if file_id in FILE_LIST:
                    FILE_LIST[file_id]["peers_with_file"] = peers_with_file

        # Update our metadata with any changes.
        update_metadata_file(new_file_data)
    except Exception as e:
        print("Peer is sending bad gossip replies: ", message , f"{e}")



def receive_full_json(sock):
    """Helper to receive a full JSON message over TCP."""
    buffer = b""
    while True:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buffer += chunk
            try:
                return json.loads(buffer.decode())
            except json.JSONDecodeError:
                continue  # Not complete yet, keep reading
        except Exception as e:
            print(f"Error during receiving: {e}")
            break
            raise ValueError("Could not parse complete JSON response.")



def is_invalid_file_response(resp):
    return (
        resp.get("file_id") in [None, "null"] or
        resp.get("file_name") in [None, "null"] or
        resp.get("data") in [None, "null"]
    )



def save_received_file(response):
    """
    Saves a file received from a peer, and updates metadata.

    Args:
        response (dict): The FILE_DATA response from a peer.

    Returns:
        bool: True if the file was successfully saved, False otherwise.
    """
    file_id = response.get("file_id")
    file_name = response.get("file_name")
    file_owner = response.get("file_owner")
    file_timestamp = response.get("file_timestamp")
    file_size = response.get("file_size")
    data = response.get("data")

    if not file_id or not file_name or not data:
        print("Invalid file response: missing file_id, file_name, or data.")
        return False

    try:
        file_path = os.path.join(DB_DIR, file_name)
        with open(file_path, "wb") as f:
            f.write(bytes.fromhex(data))
        print(f"File '{file_name}' successfully written.")

        # Update FILE_LIST metadata
        FILE_LIST[file_id] = {
            "file_id": file_id,
            "file_name": file_name,
            "file_owner": file_owner or "unknown",
            "file_timestamp": float(file_timestamp) if file_timestamp else time.time(),
            "file_size": float(file_size) if file_size else os.path.getsize(file_path),
            "has_copy": "yes",
            "peers_with_file": FILE_LIST.get(file_id, {}).get("peers_with_file", [])
        }

        save_local_files()
        update_metadata_file({file_id: FILE_LIST[file_id]})
        return True
    except Exception as e:
        print(f"Error saving file: {e}")
        return False



def get_file(file_id):
    if not file_id:
        print("Error: please enter a file id to retrieve")
        return

    # Check if the file exists locally and has a copy
    file_data = FILE_LIST.get(file_id)
    if file_data and file_data.get("has_copy") == "yes":
        print(f"You already have this file (ID: {file_id}).")
        return

    request = {
        "type": "GET_FILE",
        "file_id": file_id
    }

    # Find the peers that have the requested file
    peers_with_file = None
    for file in list(FILE_LIST.values()):
        if file_id == file.get("file_id"):
            peers_with_file = file.get("peers_with_file")
            break

    if not peers_with_file:
        print("No such file exists with any peers.")
        return
    peers_with_file = list(reversed(peers_with_file))
    tracked_peers_list = list(TRACKED_PEERS.items())

    # Try each peer that might have the file
    for (host, port), peer_info in tracked_peers_list:
        if peer_info.get("peerId") not in peers_with_file:
            continue  # This peer doesn't have the file

        try:
            with socket.create_connection((host, port), timeout=5) as s:
                s.sendall(json.dumps(request).encode())
                response = receive_full_json(s)

                if response.get("type") == "FILE_DATA":
                    success = save_received_file(response)

                    if success:
                        # After successfully saving the file, announce it to all peers
                        file_id = response.get("file_id")
                        file_name = response.get("file_name")
                        file_owner = response.get("file_owner")
                        file_timestamp = response.get("file_timestamp")
                        file_size = response.get("file_size")

                        announce_message = {
                            "type": "ANNOUNCE",
                            "from": file_owner,
                            "file_name": file_name,
                            "file_size": file_size,  # File size as received, no conversion
                            "file_id": file_id,
                            "file_owner": file_owner,
                            "file_timestamp": file_timestamp
                        }

                        # Announce the file to all tracked peers
                        for ((host, port), peer_info) in TRACKED_PEERS.items():
                            try:
                                if not send_message(host, port, json.dumps(announce_message).encode('utf-8')):
                                    print(f"[ERROR] Failed to send ANNOUNCE message to {host}:{port}")
                                else:
                                    print(f"Announced {file_name} to {host}:{port}")
                            except Exception as e:
                                print(f"Error sending ANNOUNCE to {host}:{port}: {e}")

                        return True

                    
                    else:
                        print(f"Failed to save file from peer {peer_info.get('peerId')}, trying next...")
                        continue  
                else:
                    print(f"Invalid response type from peer {peer_info.get('peerId')}, skipping...")
                    continue
        except Exception as e:
            print(f"Error retrieving file from {host}:{port}")
            continue
    return False
    print("Could not retrieve file from any peer.")



def send_file(message, sock):
    """
    Sends the file contents in response to a GET_FILE request.
    If the file is found, return the file metadata and content.
    If the file is not found, return a FILE_DATA with null/None values.
    """
    file_id = message.get("file_id")
    local_files = load_local_files()  

    # Check if the file exists in local_files
    file_info = local_files.get(file_id)

    if file_info:
            file_path = os.path.join(DB_DIR, file_info["file_name"])  # Construct the full file path using DB_DIR

            if os.path.exists(file_path):  # Check if the file exists at the given path
                with open(file_path, "rb") as f:
                    file_data = f.read()  # Read file content

                # Prepare the response message with file data in hex format
                file_data_hex = file_data.hex()
                response = {
                    "type": "FILE_DATA",
                    "file_name": file_info["file_name"],
                    "file_size": file_info["file_size"],
                    "file_id": file_info["file_id"],
                    "file_owner": file_info["file_owner"],
                    "file_timestamp": file_info["file_timestamp"],
                    "data": file_data_hex  # Convert file content to hex
                }
            else:
                # File doesn't exist at the given path, return null/None values
                response = {
                    "type": "FILE_DATA",
                    "file_name": None,
                    "file_size": None,
                    "file_id": None,
                    "file_owner": None,
                    "file_timestamp": None,
                    "data": None  # No content for invalid file
                }
    else:
            # File ID not found in local_files, return null/None values
        response = {
            "type": "FILE_DATA",
            "file_name": None,
            "file_size": None,
            "file_id": None,
            "file_owner": None,
            "file_timestamp": None,
            "data": None  # No content for invalid file
        }

    # Send the response back to the peer
    sock.send(json.dumps(response).encode())



def handle_announce(message):
    """
    Handle an incoming 'ANNOUNCE' message by updating the FILE_LIST with the new file metadata.
    """
    file_id = message.get("file_id")
    if not file_id:
        print("Received invalid ANNOUNCE message, missing file_id")
        return
    
    # Extract file details from the message
    file_name = message.get("file_name")
    file_size = message.get("file_size")
    file_owner = message.get("file_owner")
    file_timestamp = message.get("file_timestamp")
    peer_id = message.get("from")
    
    # Check if file exists in the FILE_LIST
    existing_file = FILE_LIST.get(file_id)
    
    if existing_file:
        # Update the peers_with_file list if the file exists
        if peer_id not in existing_file["peers_with_file"]:
            existing_file["peers_with_file"].append(peer_id)
            print(f"Added new peer {peer_id} for file {file_name}.")
        else:
            print(f"Peer {peer_id} already has the file {file_name}.")
    else:
        # If the file doesn't exist, add it to FILE_LIST
        FILE_LIST[file_id] = {
            "file_name": file_name,
            "file_size": file_size,
            "file_id": file_id,
            "file_owner": file_owner,
            "file_timestamp": file_timestamp,
            "peers_with_file": [peer_id],  # Start with the peer that announced it
            "has_copy": "no" 
        }
        print(f"Added new file {file_name} to the FILE_LIST.")

    # Save the updated FILE_LIST to the metadata file
    update_metadata_file({file_id: FILE_LIST[file_id]})




def handle_delete(message):

    peer = message.get("from")
    file_id = message.get("file_id")

    if not peer or not file_id:
        print(f"[ERROR] Invalid DELETE request from '{peer if peer else 'UNKNOWN'}': missing 'from' or 'file_id'")
        return

    # Check if file_id exists in the FILE_LIST
    file_entry = FILE_LIST.get(file_id)
    if not file_entry:
        print(f"[ERROR] No file found with file_id: {file_id}")
        return

    file_owner = file_entry.get("file_owner")

    # Check if the requester is the owner of the file
    if peer != file_owner:
        print(f"'{peer}' tried to delete a file that they don't own")
        return

    if file_entry.get("has_copy") == "yes":
        delete_local_file(file_id)  # Call the function to delete the local file
    
    # Remove the file from FILE_LIST
    FILE_LIST.pop(file_id, None)


    # Update the metadata file with the new FILE_LIST (after deletion)
    update_metadata_file(FILE_LIST)



def handle_connection(conn, addr):
    """Handle a single client connection in its own thread"""
    try:
        full_data = b""
        conn.settimeout(5)
        
        while True:
            chunk = conn.recv(4096)
            
            if not chunk:
                break
            full_data += chunk
            
            try:
                message = json.loads(full_data.decode())

                if not isinstance(message, dict):
                    print(f"Invalid message format from {addr}")
                    break
                msg_type = message.get("type")
                
                
                if msg_type == "GOSSIP":
                    handle_gossip(message, addr)
                elif msg_type == "GOSSIP_REPLY":
                    parse_gossip_reply(message, addr)
                elif msg_type == "GET_FILE":
                    send_file(message, conn)
                elif msg_type == "ANNOUNCE":
                    handle_announce(message)
                elif msg_type == "DELETE":
                    handle_delete(message)
                elif msg_type == "FILE_DATA":
                    save_received_file(message)
                else:
                    print(f"Unknown message type from {addr}: {msg_type}")
                break  # Exit loop if valid JSON parsed
            except json.JSONDecodeError:
                continue  # Keep reading until full message

           

    except (TimeoutError, json.JSONDecodeError) as e:
        print(f"Invalid message from {addr}: {str(e)}")
    except Exception as e:
        print(f"Error handling connection from {addr}: {str(e)}")
    finally:
        conn.close()



def handle_incoming_messages():
    """Main server loop using multi-threaded connection handling"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((LOCAL_HOST, LOCAL_PORT))
        server.listen()
        print(f"Listening on {LOCAL_HOST}:{LOCAL_PORT}")

        while True:
            try:
                conn, addr = server.accept()
                threading.Thread(
                    target=handle_connection,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except Exception as e:
                print(f"Error accepting connection: {str(e)}")


peer_lock = threading.Lock()
def cleanup_peers():
    """
    Periodically remove peers that have not been seen for a specified timeout period.
    """

    while True:
        current_time = time.time()
        
        # Collect dead peers first
        dead_peers = [peer for peer, data in TRACKED_PEERS.items()
                      if current_time - data["last_seen"] > PEER_TIMEOUT]
        
        if dead_peers:
            with peer_lock:
                # Remove dead peers
                for dead_peer in dead_peers:
                    print(f"Removing peer: {dead_peer}")
                    peer_id = TRACKED_PEERS[dead_peer]["peerId"]
                    TRACKED_PEERS.pop(dead_peer, None)  # Safely remove peer
                
                    # Now check and remove files that this peer was sharing
                    files_to_delete = []
                    for file_id, file_data in list(FILE_LIST.items()):
                        if peer_id in file_data.get("peers_with_file", []):
                            file_data["peers_with_file"].remove(peer_id)
                            
                            # If no other peer has this file, mark it for deletion
                            if not file_data["peers_with_file"]:
                                files_to_delete.append(file_id)

                    # Delete files with no peers left
                    for file_id in files_to_delete:
                        del FILE_LIST[file_id]

                # After processing, update the metadata file with changes
                update_metadata_file(FILE_LIST)

        #Sleep until next cleanup cycle
        time.sleep(CLEANUP_INTERVAL)




def upload_file(filename):
    """
    Upload a file by storing its metadata locally and announcing it to other peers
    """

    timestamp = int(time.time())
    peer_id = UMNETID

    destination_path = os.path.join(DB_DIR, os.path.basename(filename))

    try:
        with open(filename, "rb") as source_file:
            content = source_file.read()

        with open(destination_path, "wb") as dest_file:
            dest_file.write(content)

    except Exception as e:
        print(f"Error copying file contents to {DB_DIR}: {e}")
        return

    file_id = add_file_metadata(filename, UMNETID, "yes", timestamp)

    save_local_files()

    full_path = os.path.join(DB_DIR, filename) 
    file_size = os.path.getsize(full_path)

    broadcast = {
        "type": "ANNOUNCE",
        "from": peer_id,
        "file_name": filename,
        "file_size": file_size / (1024 * 1024),
        "file_id": file_id,
        "file_owner": peer_id,
        "file_timestamp": timestamp
    }

    # Rename 'list' to avoid conflict with the built-in function 'list'
    peers_list = list(TRACKED_PEERS.items())  # Corrected method call to .items()

    for ((host, port), info) in peers_list:
        if not send_message(host, port, json.dumps(broadcast).encode('utf-8')):
            print(f"[ERROR] Failed to send ANNOUNCE message to {host}:{port}")
        print(f"Announcing {filename} to {host}:{port}")
    
    peer_to_send = random.choice(peers_list)  # Randomly pick a peer
    host, port = peer_to_send[0]

    try:
        # Create a connection to the selected peer
        with socket.create_connection((host, port), timeout=5) as sock:
            # Send the file data to the peer
            send_file({"file_id": file_id}, sock)
            print(f"File '{filename}' sent to {host}:{port}")

    except Exception as e:
        print(f"Error sending file to {host}:{port} - {e}")



def send_delete(file_id):
    """
    Delete a file by checking if it exists in FILE_LIST, then sending a DELETE message to all peers
    who have the file.
    """
    
    # Check if the file_id exists in the FILE_LIST
    if file_id not in FILE_LIST:
        print(f"[ERROR] File with ID {file_id} not found in FILE_LIST")
        return  # Exit the function if the file doesn't exist
 
    
    file_info = FILE_LIST[file_id]
    # Create a copy of the peers list to avoid concurrent modification issues
    peers = file_info["peers_with_file"]
    file_name = file_info["file_name"]
    peerId = UMNETID

    delete_message = {
        "type": "DELETE",
        "from": peerId,
        "file_id": file_id
    }

    normalized_map = get_normalized_peer_map()

    for peer in peers:
        print(f"Processing peer: {peer}")
        if peer in normalized_map:
            host, port = normalized_map[peer]
            if not send_message(host, port, json.dumps(delete_message).encode('utf-8')):
                print(f"[ERROR] Failed to send DELETE message to {host}:{port}")
            else:
                print(f"Sent DELETE message for {file_name} to {peer} @ {host}:{port}")
        else:
            pass
    
    delete_local_file(file_id)
    save_local_files()


def cli_input_loop():
    """
    Command-line interface loop for user input.
    Supports commands to view peers, list files, push/get/delete files, etc.
    """
    while True:
        try:
            cmd = input("> ").strip().split()
            if not cmd:
                continue
            command = cmd[0].lower()
            if command == "peers":
               
                print("Tracked peers:")
            
                for i, ((host, port), info) in enumerate(TRACKED_PEERS.items(), 1):
                    last_seen = info["last_seen"]
                    peer_id = info["peerId"]
                    print(f"{i}. {peer_id} @ {host}:{port}, last seen: {time.time() - last_seen:.1f}s")

            elif command == "list":

                print("Current file metadata:")
                for file_id, metadata in list(FILE_LIST.items()):
                    file_name = metadata.get("file_name", "unknown")
                    peers = ", ".join(metadata.get("peers_with_file", []))
                    print(f"{file_id}: {file_name} - Peers: {peers}")

            elif command == "exit":

                os._exit(0)

            elif command == "push" and len(cmd) > 1:

                filename = cmd[1]
                 # Check if the file exists locally in the current directory
                if not os.path.isfile(filename):
                    print(f"File '{filename}' not found in the current directory.")
                    return

                upload_file(filename)

            elif command == "get" and len(cmd) > 1:

                file_id = cmd[1]
                print(f"Downloading file with id {file_id} this may take a file for large files")
                get_file(file_id)
               

            elif command == "delete" and len(cmd) > 1:
                file_id = cmd[1]

                send_delete(file_id)

            elif  command == 'ls': 
                    
                    send = False
                    print("LOCAL FILES:")
                    print("****************************************************")
                    
                    for file in os.listdir('.'):
                        if os.path.isdir(file):
                            file_type = 'Directory'
                        elif os.path.isfile(file):
                            file_type = 'File'
                        else:
                            file_type = 'Other'                                           
                        print(f"Filename: {file:<20} | Type: {file_type:<10}")
                    print("****************************************************")
                    continue 
                    
                

            elif command == "cd":
                target_dir = ' '.join(command[1:])
                current_dir = os.getcwd()
                project_root = os.path.abspath(os.path.dirname(__file__))

                if target_dir == '..':
                    parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
                    if parent_dir.startswith(project_root):
                        os.chdir(parent_dir)
                        print(f"Changed directory to: {os.getcwd()}")
                    else:
                        print("You're not allowed to go change the P2P directory.")
                else:
                    new_path = os.path.join(current_dir, target_dir)
                    if os.path.isdir(new_path):
                        os.chdir(new_path)
                        print(f"Changed directory to: {os.getcwd()}")
                    else:
                       print("You're not allowed to go change the P2P directory.")

            else:
                print(errorMess)


        except KeyboardInterrupt:
            print("\nExiting...")
            os._exit(0)


def render_stats_page():
    try:
        with open("index.html", "r") as f:
            html = f.read()

        # Replace Peer ID
        html = html.replace('id="peer-id">Loading...', f'id="peer-id">{UMNETID}')

        # Build dynamic peer table rows
        peer_rows = ""
        for (host, port), info in TRACKED_PEERS.items():
            last_seen = info["last_seen"]
            peer_id = info["peerId"]
            seconds_ago = time.time() - last_seen
            peer_rows += (
                f"<tr>"
                f"<td>{peer_id}</td>"
                f"<td>{host}</td>"
                f"<td>{port}</td>"
                f"<td>{int(seconds_ago)}s ago</td>"
                f"</tr>\n"
            )
        html = html.replace("<!-- Peer data will be inserted here -->", peer_rows)

        # Build dynamic file table rows
        file_rows = ""
        for file_id, info in list(FILE_LIST.items()):
            # Truncate file_id to the first 8 characters (or any length you prefer)
            truncated_file_id = file_id[:8]  # Only show first 8 characters

            file_name = info["file_name"]
            file_owner = info["file_owner"]
            file_size = round(info["file_size"], 3)
            has_copy = info["has_copy"]
            try:
                ts = float(info["file_timestamp"])
                # Check if the timestamp is in a reasonable range (1970 - 2100)
                if 0 < ts < 4102444800:  # January 1, 2100
                    timestamp = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    timestamp = "Invalid"
            except (ValueError, TypeError, KeyError):
                timestamp = "Invalid"
            peers_with = ", ".join(info["peers_with_file"])

            file_rows += (
                f"<tr>"
                f"<td>{truncated_file_id}..</td>"  # Display truncated file_id
                f"<td>{file_name}</td>"
                f"<td>{file_owner}</td>"
                f"<td>{file_size}</td>"
                f"<td>{timestamp}</td>"
                f"<td>{has_copy}</td>"
                f"<td>{peers_with}</td>"
                f"</tr>\n"
            )
        html = html.replace("<!-- File data will be inserted here -->", file_rows)

        return html
    except FileNotFoundError:
        return "<h1>404 Not Found</h1><p>index.html not found.</p>"


def run_stats_server():

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Allow reuse of the address and port
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((LOCAL_HOST, WEB_PORT))
    server_socket.listen(5)
    print(f"Stats server listening on {LOCAL_HOST}:{WEB_PORT}...")


    while True:
        client_socket, client_address = server_socket.accept()
        

        request = client_socket.recv(1024).decode()
    
        # Very simple parsing of GET request
        if request.startswith("GET /"):
                try:
                    
                    response_body = render_stats_page()
                    response = (
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: text/html\r\n"
                        f"Content-Length: {len(response_body.encode())}\r\n"
                        "\r\n"
                        f"{response_body}"
                    )
                except FileNotFoundError:
                    response_body = "<h1>404 Not Found</h1><p>index.html not found.</p>"
                    response = (
                        "HTTP/1.1 404 Not Found\r\n"
                        "Content-Type: text/html\r\n"
                        f"Content-Length: {len(response_body.encode())}\r\n"
                        "\r\n"
                        f"{response_body}"
                    )
        else:
            response = "HTTP/1.1 400 Bad Request\r\n\r\n"

        client_socket.sendall(response.encode())
        client_socket.close()


def sync_files():
    """
    Load local files and populate FILE_LIST with proper metadata.
    Ensures has_copy is set to "yes" and peers_with_file includes self.
    """
    local_files = load_local_files()
    for file_id, file_info in local_files.items():
        if file_id not in list(FILE_LIST):
            FILE_LIST[file_id] = {
                "file_name": file_info["file_name"],
                "file_size": file_info["file_size"],
                "file_id": file_id,
                "file_owner": file_info["file_owner"],
                "file_timestamp": file_info["file_timestamp"],
                "has_copy": "yes",
                "peers_with_file": [UMNETID],
            }
        else:
            # File is already tracked — ensure we mark has_copy and add ourself
            FILE_LIST[file_id]["has_copy"] = "yes"
            if UMNETID not in FILE_LIST[file_id]["peers_with_file"]:
                FILE_LIST[file_id]["peers_with_file"].append(UMNETID)



def automate_joining():
    """
    Automatically download the first three files from the FILE_LIST
    that the local peer doesn't already have.
    """
    local_files = load_local_files()  # Check what we already have
    count = 0

    for file_id, data in list(FILE_LIST.items()):
        if count == 3:
            break

        if file_id not in local_files:
            print(f"Downloading {data['file_name']}")
            success = get_file(file_id) 
            if success:
                count += 1
        else:
            print(f"Already have {data['file_name']}, skipping.")


def main():
    """
    Main function to start all peer services:
      - Cleanup thread for dead peers.
      - Server thread to handle incoming messages.
      - Gossip loop thread to periodically send gossip messages.
      - Command-line interface loop for user input.
    """
    sync_files()
    update_metadata_file(FILE_LIST)

    threading.Thread(target=cleanup_peers, daemon=True).start()
    threading.Thread(target=handle_incoming_messages, daemon=True).start()

    def gossip_loop():
        while True:
            print("Sending gossip...")
            send_gossip()
            time.sleep(GOSSIP_INTERVAL)
    threading.Thread(target=gossip_loop, daemon=True).start()

    threading.Thread(target=run_stats_server, daemon=True).start()

    time.sleep(1)
    print("joining the network waiting 3 seconds for gossip replies...")
    time.sleep(GOSSIP_WAIT_TIME)  # Adjust time as needed to allow gossip replies

    # Now perform the automated join process
    automate_joining()

    # Main thread handles user input
    cli_input_loop()


    cli_input_loop()  # Main thread handles user input


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python3 p2p_filesharing.py  <peer_id> [host] [p2p_port] [http_port]")
        sys.exit(1)
    UMNETID = sys.argv[1]
    LOCAL_HOST = sys.argv[2]
    LOCAL_PORT = int(sys.argv[3])
    WEB_PORT  = int(sys.argv[4])
    initialize_database()
    main()
