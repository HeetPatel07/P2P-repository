
---

# README - P2P File Sharing System  

## **Overview**
This system implements a Peer-to-Peer (P2P) File Sharing network, where each peer maintains metadata about the files it shares and tracks other peers in the network. It supports operations like listing files, uploading, downloading, and deleting files, while also utilizing a gossip protocol for peer synchronization.

### **Key Features**  
- **File Metadata**: Each file has metadata such as name, size, ID (hash of content + timestamp), owner, timestamp, and peers that have the file.
- **Peer Statistics**: Displays tracked peers and metadata of files owned by each peer.
- **Gossip Protocol**: Peers announce their presence, respond to gossip messages, and propagate file updates.
- **Web Dashboard**: View the current status of the peer, including file and peer statistics.

---

## **Running the Peer Node**  
To start the peer node, use the following command:  
```bash
python3 p2p_filesharing.py <UMNET_ID> <FULL_HOSTNAME> <P2P_PORT> <HTTP_PORT>
```  

**Example**:  
```bash
python3 p2p_filesharing.py patelhs hawk.cs.umanitoba.ca 8195 8196
```  

### **Port Requirements**  
- **P2P Port (8195-8199)**: Peer communication port (e.g., `8195`)  
- **HTTP Port (8195-8199)**: Web dashboard port (e.g., `8196`)  
*Ensure you use distinct ports if running multiple peers on the same host.*

---

## **Web Dashboard Access**  
To view your peer’s network statistics, visit:  
```  
http://<FULL_HOSTNAME>:<HTTP_PORT>  
```  
**Example**:  
```  
http://hawk.cs.umanitoba.ca:8196  
```  

---

## **Multi-Node Deployment**  
To run multiple peers on different servers:  
```bash
# First peer (hawk):
python3 p2p_filesharing.py patelhs hawk.cs.umanitoba.ca 8195 8196

# Second peer (eagle):
python3 p2p_filesharing.py patelhs eagle.cs.umanitoba.ca 8197 8198
```  

---

## **Core Commands**  
These are the core operations that can be performed by the peer node:

| Command            | Action                                      |  
|--------------------|---------------------------------------------|  
| `list`             | List all files available on the network.    |  
| `push <Path>`      | Upload a file and announce it to the network (e.g., `push file.txt`)        |  
| `get <file_id>`    | Download a file from the network (e.g., `get <file_id>`)     |  
| `delete <file_id>` | Delete a file (if you are the owner of the file). The delete command will also be sent to peers with the file. However, even if you are not the owner, the file will be deleted locally. Other peers will choose not to delete the file if you are not the owner. |  
| `exit`             | Exit the program.                           |  

---

## **Special Operations**  
These operations involve synchronization across peers:  

- **`push <Path>`**:  
  After uploading the file, it is also shared with one other tracked peer. The new file is announced to the network.

- **`get <file_id>`**:  
  Announce the local file to the other peers to ensure synchronization.

- **`delete <file_id>`**:  
  Deletes the file locally and sends a delete command to all peers that have the file locally. Only the owner can delete the file from other peers, but the file will be deleted locally on the peer that initiated the delete, regardless of ownership.

---

## **Peer Management**  
Tracked peers can be viewed using the `peers` command. If a peer is unresponsive for a certain period, it is dropped from the list.

---

## **Approved Server Hostnames**  
Your peer can connect to one of the following well-known servers for initial gossiping:  
```  
silicon.cs.umanitoba.ca:8999  
eagle.cs.umanitoba.ca:8999  
grebe.cs.umanitoba.ca:8999  
hawk.cs.umanitoba.ca:8999  
```  

---

## **Metadata and Synchronization**

### **File Metadata**  
Your peer tracks the following metadata for each file:

- **Name**  
- **Size**  
- **File ID (SHA-256 hash)**  
- **Owner**  
- **Timestamp**  
- **Peers with file**

The metadata is updated based on `GOSSIP_REPLY` and `ANNOUNCE` messages received from other peers.

### **Peer Cleanup**  
Inactive or unresponsive peers are automatically cleaned up after not being heard from for a minute.

---
---
