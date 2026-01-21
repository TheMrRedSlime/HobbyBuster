import socket, struct, gzip, io, os, threading, time, numpy as np
import requests, json, random, string, hashlib
from pyngrok import ngrok
import ujson as json


# World configuration
X, Y, Z = 2560, 128, 2560
clients = {}
clients_lock = threading.Lock()
next_player_id = 0
admin_list = ["TheMrRedSlime"]
USER_DB_FILE = "users.json"
player_list = set()
authenticated_clients = set()

# LOG-BASED SYSTEM: Stores (index, block_type) to avoid 2GB RAM usage
block_logs = {} 
logs_lock = threading.Lock()

def hash_password(password):
    return hashlib.sha512(password.encode()).hexdigest()

def load_users():
    if os.path.exists(USER_DB_FILE):
        with open(USER_DB_FILE, 'r') as f: return json.load(f)
    return {}

def save_user(username, password_hash):
    users = load_users()
    users[username.lower()] = password_hash
    with open(USER_DB_FILE, 'w') as f: json.dump(users, f)

def pad_string(s):
    return s[:64].ljust(64).encode('ascii')

def recv_exact(sock, n, max_size=1024):
    if n > max_size:
        raise ValueError(f"Security Trigger: Packet size {n} exceeds limit {max_size}")
        
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk: raise ConnectionError("Closed")
        data += chunk
    return data
    
def generate_initial_rle():
    """Creates a new world.rle with half grass if none exists."""
    if os.path.exists("world.rle"): return
    print("[SERVER] Generating initial RLE world (Half Grass)...")
    half = (X * Y * Z) // 2
    with open("world.rle", "wb") as f:
        # Write in chunks to avoid memory issues
        chunk_size = 10000000  # 10M blocks at a time
        remaining_grass = half
        
        while remaining_grass > 0:
            count = min(255, remaining_grass)
            f.write(bytes([count, 2]))  # Grass
            remaining_grass -= count
        
        remaining_air = (X * Y * Z) - half
        while remaining_air > 0:
            count = min(255, remaining_air)
            f.write(bytes([count, 0]))  # Air
            remaining_air -= count
    
    print("[SERVER] RLE world generated!")

def auto_save_task():
    while True:
        time.sleep(300) 
        if not block_logs: continue
        
        print(f"[SERVER] Fast-saving {len(block_logs)} changes in 50MB chunks...")
        try:
            with logs_lock:
                changes_copy = dict(block_logs)
            
            CHUNK_SIZE = 50_000_000 
            new_rle_path = "world.rle.tmp"
            
            with open("world.rle", "rb") as f_in, open(new_rle_path, "wb") as f_out:
                # Read all existing RLE metadata at once (this is small)
                rle_data = f_in.read()
                counts = np.frombuffer(rle_data[0::2], dtype=np.uint8)
                vals = np.frombuffer(rle_data[1::2], dtype=np.uint8)
                
                # Reconstruct only the indices we need to find chunk boundaries
                # Note: We don't reconstruct the world, just the "cumulative" counts
                cum_counts = np.cumsum(counts, dtype=np.int64)
                
                for start in range(0, X*Y*Z, CHUNK_SIZE):
                    end = min(start + CHUNK_SIZE, X*Y*Z)
                    
                    # Find which RLE pairs belong to this 100MB chunk
                    # This is the "secret sauce" for speed
                    idx_start = np.searchsorted(cum_counts, start, side='right')
                    idx_end = np.searchsorted(cum_counts, end, side='left')
                    
                    # Materialize just THIS 100MB chunk
                    chunk_counts = counts[idx_start:idx_end+1].copy()
                    chunk_vals = vals[idx_start:idx_end+1].copy()
                    
                    # Adjust the first and last run if they overlap chunk boundaries
                    chunk_counts[0] = cum_counts[idx_start] - start
                    chunk_counts[-1] = end - (cum_counts[idx_end-1] if idx_end > 0 else 0)
                    
                    # Expand, patch, and re-compress the chunk using NumPy
                    temp_chunk = np.repeat(chunk_vals, chunk_counts)
                    
                    # Patch logs
                    for idx in list(changes_copy.keys()):
                        if start <= idx < end:
                            temp_chunk[idx - start] = changes_copy.pop(idx)
                    
                    # Fast RLE compression for the chunk
                    if len(temp_chunk) > 0:
                        diffs = np.concatenate(([True], temp_chunk[1:] != temp_chunk[:-1]))
                        points = np.where(diffs)[0]
                        run_vals = temp_chunk[points]
                        run_lens = np.diff(np.append(points, len(temp_chunk)))
                        
                        for c, v in zip(run_lens, run_vals):
                            while c > 255:
                                f_out.write(bytes([255, int(v)]))
                                c -= 255
                            f_out.write(bytes([int(c), int(v)]))

            os.replace(new_rle_path, "world.rle")
            with logs_lock:
                for key in (set(block_logs.keys()) - set(changes_copy.keys())):
                    block_logs.pop(key, None)
            print("[SERVER] Fast-save complete.")
        except Exception as e:
            print(f"[ERROR] Save failed: {e}")

def handle_command(player_name: str, message: str, client_socket):
    args = message.split()
    command = args[0].lower()
    if command == "/kick":
        if player_name not in admin_list:
            send_message(client_socket, "&cYou do not have permission to use this command!")
            return
        if len(args) < 2:
            send_message(client_socket, "&cUsage: /kick <player> [reason]")
            return
        target = args[1]
        reason = ' '.join(args[2:]) if len(args) > 2 else "Kicked by operator"
        target_socket = None
        with clients_lock:
            for sock, (name, pid) in clients.items():
                if name.lower() == target.lower():
                    target_socket = sock
                    break

        if target_socket:
            print(f"[KICK] {player_name} kicked {target}: {reason}")
            packet = struct.pack('>BB', 0x0d, 0xff)
            packet += pad_string(f"&e{target} was kicked: {reason}")
            broadcast(packet)
            kick_player(target_socket, reason)
        else:
            send_message(client_socket, f"&cPlayer '{target}' not found")
    elif command == "/register":
        if len(args) < 2:
            send_message(client_socket, "&cUsage: /register <password>")
            return
        users = load_users()
        if player_name.lower() in users:
            send_message(client_socket, "&cYou are already registered! Use /login.")
            return
        save_user(player_name, hash_password(args[1]))
        authenticated_clients.add(client_socket)
        send_message(client_socket, "&aRegistered and logged in successfully!")

    elif command == "/login":
        if len(args) < 2:
            send_message(client_socket, "&cUsage: /login <password>")
            return
        users = load_users()
        pw_hash = hash_password(args[1])
        if users.get(player_name.lower()) == pw_hash:
            authenticated_clients.add(client_socket)
            send_message(client_socket, "&aLogged in! You can now move and speak.")
        else:
            send_message(client_socket, "&cInvalid password!")
    else:
        send_message("&cCommand not found!")
        

def send_message(client_socket, message):
    """Send a message to a specific client"""
    try:
        packet = struct.pack('>BB', 0x0d, 0xff)
        packet += pad_string(message)
        client_socket.sendall(packet)
    except:
        pass

def teleport_player(client_socket, x, y, z, yaw=0, pitch=0):
    """Teleport a player to specific coordinates"""
    try:
        # [0x08][player_id:byte][x:short][y:short][z:short][yaw:byte][pitch:byte]
        packet = struct.pack('>BbhhhBB', 0x08, -1, x, y, z, yaw, pitch)
        client_socket.sendall(packet)
    except:
        pass

def kick_player(target_socket, reason):
    """Kick a player from the server"""
    try:
        # [0x0e][reason*64]
        packet = struct.pack('>B', 0x0e)
        packet += pad_string("Kicked for reason: " + reason)
        target_socket.sendall(packet)
        target_socket.close()
    except:
        pass

def handle_client(client_socket, address):
    global next_player_id
    player_id = -1
    player_name = "Unknown"
    
    try:
        print(f"[CONNECT] Connection from {address}")
        
        # --- 1. Handshake & Identification ---
        handshake_id = recv_exact(client_socket, 1)[0]
        if handshake_id != 0x00:
            print(f"[ERROR] Bad handshake: {handshake_id}")
            return
        
        protocol_version = recv_exact(client_socket, 1)[0]
        player_name = recv_exact(client_socket, 64).decode('ascii').strip()
        verify_key = recv_exact(client_socket, 64)
        unused = recv_exact(client_socket, 1)
        
        print(f"[LOGIN] {player_name} (Protocol {protocol_version})")
        
        # Assign player ID
        with clients_lock:
            player_id = next_player_id
            next_player_id = (next_player_id + 1) % 128
            if next_player_id == 255:  # Skip 255 (reserved)
                next_player_id = 0
            clients[client_socket] = (player_name, player_id)

        # Send Server Identification [0x00]
        packet = struct.pack('BB', 0x00, 0x07)
        packet += pad_string("RLE Server")
        packet += pad_string("Direct-Stream")
        packet += struct.pack('B', 0x00)  # User type
        client_socket.sendall(packet)
        
        # --- 2. Level Streaming (DIRECT - NO RAM EXPANSION) ---
        client_socket.sendall(struct.pack('B', 0x02))  # Level Initialize

        # Custom streamer that pipes RLE→Gzip→Socket without loading full world
        class DirectSocketStreamer:
            def __init__(self, sock):
                self.sock = sock
                self.sent = 0
                self.total_size = X * Y * Z
            
            def write(self, data):
                # Send data in 1024-byte chunks
                for i in range(0, len(data), 1024):
                    chunk = data[i:i+1024]
                    self.sent += len(chunk)
                    percent = min(100, int((self.sent / self.total_size) * 100))
                    
                    # Packet 0x03: [ID][Length:short][Data*1024][Percent]
                    packet = struct.pack('>BH', 0x03, len(chunk))
                    packet += chunk.ljust(1024, b'\x00')
                    packet += struct.pack('B', percent)
                    self.sock.sendall(packet)

        # Stream world directly from RLE file without expanding in RAM
        streamer = DirectSocketStreamer(client_socket)
        with gzip.GzipFile(fileobj=streamer, mode='wb', compresslevel=6) as gz:
            # Write world size header
            gz.write(struct.pack('>I', X * Y * Z))
            
            # Stream RLE data - expand on-the-fly
            with open("world.rle", "rb") as f:
                while True:
                    pair = f.read(2)
                    if not pair or len(pair) < 2:
                        break
                    count = pair[0]
                    block_id = pair[1]
                    # Expand this RLE pair directly into gzip stream
                    gz.write(bytes([block_id]) * count)
        
        # Level Finalize [0x04]
        packet = struct.pack('>Bhhh', 0x04, X, Y, Z)
        client_socket.sendall(packet)
        
        # --- 3. Player Spawning ---
        # Classic protocol uses signed shorts (-32768 to 32767)
        # For large worlds, spawn near origin instead of center
        if (X // 2) * 32 > 32767 or (Z // 2) * 32 > 32767:
            # Spawn at a safe location within protocol limits
            spawn_x = 512 * 32  # Block 512
            spawn_y = 70 * 32   # Block 70 (above ground)
            spawn_z = 512 * 32  # Block 512
        else:
            # Normal spawn at world center
            spawn_x = (X // 2) * 32
            spawn_y = ((Y // 2) + 10) * 32
            spawn_z = (Z // 2) * 32
        
        # Tell ALL existing players about the new player FIRST
        spawn_packet_others = struct.pack('>Bb', 0x07, player_id)
        spawn_packet_others += pad_string(player_name)
        spawn_packet_others += struct.pack('>hhhBB', spawn_x, spawn_y, spawn_z, 0, 0)
        broadcast(spawn_packet_others, exclude=client_socket)
        
        # Tell new player about themselves
        spawn_packet_self = struct.pack('>Bb', 0x07, -1)  # -1 = self
        spawn_packet_self += pad_string(player_name)
        spawn_packet_self += struct.pack('>hhhBB', spawn_x, spawn_y, spawn_z, 0, 0)
        client_socket.sendall(spawn_packet_self)
        
        # Tell new player about all existing players
        with clients_lock:
            for sock, (name, pid) in clients.items():
                if sock != client_socket:
                    other_spawn = struct.pack('>Bb', 0x07, pid)
                    other_spawn += pad_string(name)
                    other_spawn += struct.pack('>hhhBB', spawn_x, spawn_y, spawn_z, 0, 0)
                    client_socket.sendall(other_spawn)
        
        # Join message
        join_msg = struct.pack('>BB', 0x0d, 0xff)
        join_msg += pad_string(f"&e{player_name} joined the game")
        player_list.add(player_name)

        broadcast(join_msg)
        
        print(f"[SPAWN] {player_name} spawned (ID: {player_id})")
        
        # --- 4. Main Packet Loop ---
        move_packet_count = 0
        blocks_placed = 0
        last_check_time = time.time()
        last_grief_time = time.time()
        send_message(client_socket, "&ePlease /login <password> or /register <password>")
        while True:
            packet_id = recv_exact(client_socket, 1)[0]
            VALID_PIDS = {0x00, 0x05, 0x08, 0x0d} 
            is_auth = client_socket in authenticated_clients
            if packet_id not in VALID_PIDS:
                print(f"[SECURITY] Invalid packet ID 0x{packet_id:02x} from {player_name}. Blocking.")
                kick_player(client_socket, "Invalid packet sequence detected.")
                return

            if time.time() - last_grief_time >= 1:
                if blocks_placed > 45:
                    print(f"[SECURITY] Triggered Anti Grief System")
                    kick_player(client_socket, "Triggered Anti Grief. Slow down!")
                    blocks_placed = 0
                    last_grief_time = time.time()
                blocks_placed = 0
                last_check_time = time.time()

            if time.time() - last_check_time >= 30:
                #print(f"[NETWORK] {player_name} sent {move_packet_count} move packets in the last 30 seconds.")
                if move_packet_count > (30*20)+(30*2):
                    kick_player(client_socket, "Triggered Packet Spam")
                move_packet_count = 0
                last_check_time = time.time()
            
            if packet_id == 0x05:  # Set Block
                x = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                y = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                z = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                mode = recv_exact(client_socket, 1)[0]
                block_type = recv_exact(client_socket, 1)[0]
                
                
                # Validate coordinates
                if 0 <= x < X and 0 <= y < Y and 0 <= z < Z:
                    idx = (y * Z + z) * X + x
                    new_block = block_type if mode == 1 else 0
                    
                    # Store in log
                    with logs_lock: 
                        block_logs[idx] = new_block
                    
                    # Broadcast block change [0x06]
                    blocks_placed += 1
                    block_packet = struct.pack('>BhhhB', 0x06, x, y, z, new_block)
                    broadcast(block_packet)
            
            elif packet_id == 0x08:  # Position & Orientation

                move_packet_count +=1
                pid = recv_exact(client_socket, 1)[0]  # Player's own ID (ignored)
                x = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                y = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                z = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                yaw = recv_exact(client_socket, 1)[0]
                pitch = recv_exact(client_socket, 1)[0]
                
                if not is_auth:
                    teleport_player(client_socket, spawn_x, spawn_y, spawn_z)
                    continue
                
                # Broadcast position with SERVER's assigned player_id
                move_packet = struct.pack('>BbhhhBB', 0x08, player_id, x, y, z, yaw, pitch)
                broadcast(move_packet, exclude=client_socket)
            
            elif packet_id == 0x0d:  # Message
                pid = recv_exact(client_socket, 1)[0]
                message = recv_exact(client_socket, 64).decode('ascii').strip()

                if not is_auth and not (message.startswith('/login') or message.startswith('/register')):
                    send_message(client_socket, "&cLogin to chat!")
                    continue
                
                if message:
                    if message.startswith('/'):
                        print(f"[COMMAND] <{player_name}> <{message}>")
                        handle_command(player_name, message, client_socket)
                    else:
                        print(f"[CHAT] <{player_name}> {message}")
                        chat_packet = struct.pack('>BB', 0x0d, 0xff)
                        chat_packet += pad_string(f"&f<{player_name}> {message}")
                        broadcast(chat_packet)
            
            else:
                print(f"[WARN] Unknown packet 0x{packet_id:02x} from {player_name}")

    except ConnectionError:
        print(f"[DISCONNECT] {player_name} connection lost")
    except Exception as e:
        print(f"[ERROR] {player_name}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        with clients_lock:
            if client_socket in clients:
                del clients[client_socket]
        
        # Despawn player
        if player_id >= 0:
            despawn_packet = struct.pack('>Bb', 0x0c, player_id)
            broadcast(despawn_packet)
            
            leave_msg = struct.pack('>BB', 0x0d, 0xff)
            leave_msg += pad_string(f"&e{player_name} left the game")
            broadcast(leave_msg)

            player_list.remove(player_name)
            
            print(f"[LEAVE] {player_name} left")
        
        try:
            client_socket.close()
        except:
            pass

def broadcast(packet, exclude=None):
    """Send packet to all clients except excluded one"""
    with clients_lock:
        dead_sockets = []
        for sock in list(clients.keys()):
            if sock != exclude:
                try:
                    sock.sendall(packet)
                except:
                    dead_sockets.append(sock)
        
        # Clean up dead connections
        for sock in dead_sockets:
            if sock in clients:
                del clients[sock]

def main():
    generate_initial_rle()
    
    # Start auto-save thread
    threading.Thread(target=auto_save_task, daemon=True).start()
    print("[SERVER] Auto-save enabled (every 5 minutes)")

    try:
        tunnel = ngrok.connect(25565, "tcp")
        print(f"[NGROK] Tunnel established: {tunnel.public_url}")
        
    except Exception as e:
        print(f"[NGROK ERROR] {e}")

    # Start server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', 25565))
    server.listen(5)
    
    print(f"[SERVER] RLE Log Server Running")
    print(f"[SERVER] World: {X}x{Y}x{Z} = {X*Y*Z:,} blocks")
    print(f"[SERVER] Listening on port 25565...")
    
    try:
        while True:
            client_sock, client_addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(client_sock, client_addr), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down...")
        if not block_logs: pass
        
        print(f"[SERVER] Fast-saving {len(block_logs)} changes in 50MB chunks...")
        try:
            with logs_lock:
                changes_copy = dict(block_logs)
            
            CHUNK_SIZE = 50_000_000 
            new_rle_path = "world.rle.tmp"
            
            with open("world.rle", "rb") as f_in, open(new_rle_path, "wb") as f_out:
                # Read all existing RLE metadata at once (this is small)
                rle_data = f_in.read()
                counts = np.frombuffer(rle_data[0::2], dtype=np.uint8)
                vals = np.frombuffer(rle_data[1::2], dtype=np.uint8)
                
                # Reconstruct only the indices we need to find chunk boundaries
                # Note: We don't reconstruct the world, just the "cumulative" counts
                cum_counts = np.cumsum(counts, dtype=np.int64)
                
                for start in range(0, X*Y*Z, CHUNK_SIZE):
                    end = min(start + CHUNK_SIZE, X*Y*Z)
                    
                    # Find which RLE pairs belong to this 100MB chunk
                    # This is the "secret sauce" for speed
                    idx_start = np.searchsorted(cum_counts, start, side='right')
                    idx_end = np.searchsorted(cum_counts, end, side='left')
                    
                    # Materialize just THIS 100MB chunk
                    chunk_counts = counts[idx_start:idx_end+1].copy()
                    chunk_vals = vals[idx_start:idx_end+1].copy()
                    
                    # Adjust the first and last run if they overlap chunk boundaries
                    chunk_counts[0] = cum_counts[idx_start] - start
                    chunk_counts[-1] = end - (cum_counts[idx_end-1] if idx_end > 0 else 0)
                    
                    # Expand, patch, and re-compress the chunk using NumPy
                    temp_chunk = np.repeat(chunk_vals, chunk_counts)
                    
                    # Patch logs
                    for idx in list(changes_copy.keys()):
                        if start <= idx < end:
                            temp_chunk[idx - start] = changes_copy.pop(idx)
                    
                    # Fast RLE compression for the chunk
                    if len(temp_chunk) > 0:
                        diffs = np.concatenate(([True], temp_chunk[1:] != temp_chunk[:-1]))
                        points = np.where(diffs)[0]
                        run_vals = temp_chunk[points]
                        run_lens = np.diff(np.append(points, len(temp_chunk)))
                        
                        for c, v in zip(run_lens, run_vals):
                            while c > 255:
                                f_out.write(bytes([255, int(v)]))
                                c -= 255
                            f_out.write(bytes([int(c), int(v)]))

            os.replace(new_rle_path, "world.rle")
            with logs_lock:
                for key in (set(block_logs.keys()) - set(changes_copy.keys())):
                    block_logs.pop(key, None)
            print("[SERVER] Fast-save complete.")
        except Exception as e:
            print(f"[ERROR] Save failed: {e}")
        
        server.close()

if __name__ == "__main__":
    main()