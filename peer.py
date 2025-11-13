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
    with open(LOCAL_FILES_PATH, "r") as f:
        return json.load(f)



def generate_file_id(filename, timestamp):
    """
    Generate a unique file ID using the file's content and a provided timestamp.
    """
    # Get the file content (you'll need to read the file to get its content)
    with open(filename, 'rb') as f:
        content = f.read()

    # Hash the content and the provided timestamp (not the current time)
    hashBase = hashlib.sha256()
    hashBase.update(content)
    hashBase.update(str(timestamp).encode())  # Use the provided timestamp

    # Return the hexadecimal hash
    return hashBase.hexdigest()


def add_file_metadata(filename, peer_id, timestamp=None):
    """
    Add a new file's metadata to the FILE_LIST, including the generated file_id.
    If no timestamp is provided, use the current time as fallback.
    """
    if timestamp is None:
        timestamp = int(time.time())  # Use the current time if no timestamp is provided

    file_id = generate_file_id(filename, timestamp)  # Generate file_id using the provided timestamp
    file_size = os.path.getsize(filename)

    FILE_LIST[file_id] = {
        "file_name": filename,
        "file_size": file_size,
        "file_id": file_id,
        "file_owner": peer_id,
        "file_timestamp": timestamp,  # Store the original timestamp
        "peers_with_file": [peer_id],  # Track which peers have the file
        "has_copy": "yes"  # Assuming the file was uploaded or received
    }

    


def save_local_files():
    """
    Save the current local file metadata to LOCAL_FILES_PATH.
    This function ensures the file list is locked while saving.
    """
    with metadata_lock:
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
LOCAL_HOST = "hawk.cs.umanitoba.ca"  # Set to your IP if testing across machines

# Constants for gossiping and peer management
GOSSIP_INTERVAL = 30    # Seconds between gossip messages
PEER_TIMEOUT = 60       # Seconds before a peer is considered dead
CLEANUP_INTERVAL = 30   # Seconds between cleaning up dead peers
MAX_FORWARD_PEERS = 2   # Maximum number of peers to forward a gossip message to

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

# Define well-known bootstrap peers by hostname, then resolve them to IP addresses.
WELL_KNOWN_HOSTS_RAW = [("130.179.28.113", 8999), ("130.179.28.37", 8999)]
WELL_KNOWN_HOSTS = resolve_hosts(WELL_KNOWN_HOSTS_RAW)

####################################################################
####################################################################
####################################################################





# Dictionary to track peers: key=(ip, port), value=last seen timestamp
TRACKED_PEERS = {}
# Global sets to track gossip messages for duplicate prevention.
SENT_GOSSIPS = set()
RECEIVED_GOSSIP_IDS = {}


####################################################################
########################### Messaging Functions ####################
####################################################################
def send_message(host, port, data):
    """
    Create a TCP connection to the given host and port and send the provided data.
    Returns True on success, False on failure.
    """
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            sock.sendall(data)
        return True 
    except Exception as e:
        return False



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
            if gossip_id not in SENT_GOSSIPS:
                send_message(peer[0], peer[1], json.dumps(message).encode())
                SENT_GOSSIPS.add(gossip_id)



def send_gossip():
    """
    Generate and send a gossip message to well-known hosts and then forward it to random peers.
    """
    gossip_id = str(uuid.uuid4())
    message = {
        "type": "GOSSIP",
        "host": LOCAL_HOST,
        "port": LOCAL_PORT,
        "id": gossip_id,
        "peerId": UMNETID
    }
    
    # Prevent sending if already sent.
    if gossip_id in SENT_GOSSIPS:
        return
    SENT_GOSSIPS.add(gossip_id)

    data = json.dumps(message).encode()

    # Send to all well-known bootstrap peers.
    for host, port in WELL_KNOWN_HOSTS:
        send_message(host, port, data)

    # Also forward to random peers (excluding well-known hosts).
    forward_gossip_to_peers(gossip_id, message)



def handle_gossip(message, sender_addr):
    """
    Process an incoming gossip message: update peer info, forward it to others,
    and send a GOSSIP_REPLY back to the sender.
    """
    gossip_id = message["id"]
    peer_key = (message["host"], message["port"])
    
    # Initialize tracking set for this peer if needed.
    if peer_key not in RECEIVED_GOSSIP_IDS:
        RECEIVED_GOSSIP_IDS[peer_key] = set()
    
    # If this gossip message was already processed, ignore it.
    if gossip_id in RECEIVED_GOSSIP_IDS[peer_key]:
        return

    # Mark the gossip message as received from this peer.
    RECEIVED_GOSSIP_IDS[peer_key].add(gossip_id)
    
    # Update last seen time for this peer.
    TRACKED_PEERS[peer_key] = {
        "last_seen": time.time(),
        "peerId": message.get("peerId", "UnknownPeer")
    }

    # Forward this gossip message to other peers (exclude the originator).
    forward_gossip_to_peers(gossip_id, message, sender_peer=peer_key)

    # Send a reply back to the originator with our file metadata.
    reply = {
        "type": "GOSSIP_REPLY",
        "host": LOCAL_HOST,
        "port": LOCAL_PORT,
        "peerId": UMNETID,
        "files": []  # You can populate this with your actual file metadata when implemented.
    }
    send_message(message["host"], message["port"], json.dumps(reply).encode())



def parse_gossip_reply(message, sender_addr):
    """Parse incoming GOSSIP_REPLY messages and update metadata."""
    peer_key = (message["host"], message["port"])
    
    TRACKED_PEERS[peer_key] = {
        "last_seen": time.time(),
        "peerId": message.get("peerId", "UnknownPeer")
    }

    peer_id = message.get("peerId", "UnknownPeer")
    files = message.get("files", [])

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

                # If new timestamp exists and is later than the old one, update metadata.
                if new_ts_str and (not old_ts_str or new_ts_str > old_ts_str):
                    update_needed = True
                else:
                    # Otherwise, keep the existing data.
                    update_needed = False
            else:
                # If file not in our list, add it.
                update_needed = True

            # Decide on 'has_copy' based solely on whether we have retrieved the file locally.
            has_copy = FILE_LIST.get(file_id, {}).get("has_copy", "no")
            # Ensure we add the current peer to the peers list
            peers_with_file = FILE_LIST.get(file_id, {}).get("peers_with_file", [])
            if peer_id not in peers_with_file:
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
    except Exception as e :
        print("Peer is sending bad gossip replies: ", message.get("peer_id"))


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



def get_file(file_id):
    if not file_id:
        print("Error: please enter a file id to retrieve")
        return

    request = {
        "type": "GET_FILE",
        "file_id": file_id
    }

    peers_with_file = None
    for file in FILE_LIST.values():
        if file_id == file.get("file_id"):
            peers_with_file = file.get("peers_with_file")

    if not peers_with_file:
        print("No such file exists with any peers")
        return

    tracked_peers_list = list(TRACKED_PEERS.items())

    for (host, port), peer_info in tracked_peers_list:
        for peer in peers_with_file:
            if peer_info.get("peerId") == peer:
                try:
                    with socket.create_connection((host, port), timeout=5) as s:
                        s.sendall(json.dumps(request).encode())

                        response = receive_full_json(s)

                        if response.get("type") == "FILE_DATA":
                            if not response.get("file_id"):
                                print("File not found on peer.")
                                return
                            file_path = os.path.join(DB_DIR, response["file_name"])

                            with open(file_path, "wb") as f:
                                f.write(bytes.fromhex(response["data"]))
                            print("File successfully written.")

                            FILE_LIST[file_id]["has_copy"] = "yes"
                            save_local_files()
                            update_metadata_file({file_id: FILE_LIST[file_id]})
                            return
                        else:
                            print("Invalid response from peer.")
                            return
                except Exception as e:
                    print(f"Error retrieving file from {host}:{port} -> {e}")
    print("Could not retrieve file from any peer.")







def handle_incoming_messages():
    """
    Main server loop that listens for incoming TCP connections and routes messages
    to the appropriate handlers based on their 'type'.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((LOCAL_HOST, LOCAL_PORT))
        server.listen()
        print("Listening on", {LOCAL_HOST}, {LOCAL_PORT})

        input_sockets = [server]
        while True:
            readable, _, _ = select.select(input_sockets, [], [], 1)
            for sock in readable:
                if sock == server:
                    conn, addr = server.accept()
                    input_sockets.append(conn)
                else:
                    try:
                        data = sock.recv(1024).decode()
                        if data:
                            message = json.loads(data)
                            msg_type = message.get("type")
                            if msg_type == "GOSSIP":
                                handle_gossip(message, addr)
                            elif msg_type == "GOSSIP_REPLY":
                                parse_gossip_reply(message, addr)
                            else:
                                print("Received unknown message:", message)
                        sock.close()
                        input_sockets.remove(sock)
                    except (json.JSONDecodeError, ConnectionResetError):
                        input_sockets.remove(sock)
                        sock.close()





def cleanup_peers():
    """
    Periodically remove peers that have not been seen for a specified timeout period.
    """
    while True:
        current_time = time.time()
        dead_peers = [peer for peer, data in TRACKED_PEERS.items()
                      if current_time - data["last_seen"] > PEER_TIMEOUT]
        for peer in dead_peers:
            TRACKED_PEERS.pop(peer)
        time.sleep(CLEANUP_INTERVAL)




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
                print("Tracked peers:")

                for i, ((host, port), info) in enumerate(TRACKED_PEERS.items(), 1):
                    last_seen = info["last_seen"]
                    peer_id = info["peerId"]
                    print(f"{i}. {peer_id} @ {host}:{port}, last seen: {time.time() - last_seen:.1f}s")

            elif command == "list":
                print("Current file metadata:")
                print(json.dumps(FILE_LIST, indent=4))
            elif command == "exit":
                os._exit(0)
            elif command == "push" and len(cmd) > 1:
                filename = cmd[1]
                print(f"Uploading {filename} (Feature not implemented yet)")
            elif command == "get" and len(cmd) > 1:
                file_id = cmd[1]
                get_file(file_id)
                print(f"Downloading file with id {file_id}")
            elif command == "delete" and len(cmd) > 1:
                file_id = cmd[1]
                print(f"Deleting file with id {file_id} (Feature not implemented yet)")
            else:
                print(errorMess)
        except KeyboardInterrupt:
            print("\nExiting...")
            os._exit(0)



def main():
    """
    Main function to start all peer services:
      - Cleanup thread for dead peers.
      - Server thread to handle incoming messages.
      - Gossip loop thread to periodically send gossip messages.
      - Command-line interface loop for user input.
    """
    threading.Thread(target=cleanup_peers, daemon=True).start()
    threading.Thread(target=handle_incoming_messages, daemon=True).start()

    def gossip_loop():
        while True:
            print("Sending gossip...")
            send_gossip()
            time.sleep(GOSSIP_INTERVAL)
    threading.Thread(target=gossip_loop, daemon=True).start()

    cli_input_loop()  # Main thread handles user input

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 peer.py <portnum> <UMNETID>")
        sys.exit(1)

    LOCAL_PORT = int(sys.argv[1])
    UMNETID = sys.argv[2]
    initialize_database()
    main()
