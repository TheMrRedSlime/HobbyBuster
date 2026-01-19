import socket
import struct
import gzip
import io
import os
import lzma
import threading
import time

# World configuration
X, Y, Z = 128, 64, 128
world_data = bytearray(X * Y * Z)
clients = {}
clients_lock = threading.Lock()
next_player_id = 0

def pad_string(s):
    """Pad string to 64 bytes"""
    return s[:64].ljust(64).encode('ascii')

def recv_exact(sock, n):
    """Receive exactly n bytes or raise exception"""
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed")
        data += chunk
    return data

def save_world():
    """Save world to disk"""
    try:
        with lzma.open("world.dat", "wb", preset=9 | lzma.PRESET_EXTREME) as f:
            f.write(world_data)
        print("[SERVER] World saved")
    except Exception as e:
        print(f"[ERROR] Failed to save world: {e}")

def broadcast(packet, exclude_socket=None):
    """Send packet to all connected clients"""
    with clients_lock:
        dead_sockets = []
        for sock, (name, pid) in list(clients.items()):
            if sock != exclude_socket:
                try:
                    sock.sendall(packet)
                except:
                    dead_sockets.append(sock)
        
        # Clean up dead connections
        for sock in dead_sockets:
            if sock in clients:
                name, pid = clients[sock]
                del clients[sock]
                print(f"[SERVER] Cleaned up dead connection: {name}")

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

def handle_command(client_socket, player_name, message):
    """Handle player commands"""
    parts = message.split()
    command = parts[0].lower()
    
    if command == '/kick':
        if len(parts) < 2:
            send_message(client_socket, "&cUsage: /kick <player> [reason]")
            return
        
        target_name = parts[1]
        reason = ' '.join(parts[2:]) if len(parts) > 2 else "Kicked by operator"
        
        # Find target player
        target_socket = None
        with clients_lock:
            for sock, (name, pid) in clients.items():
                if name.lower() == target_name.lower():
                    target_socket = sock
                    break
        
        if target_socket:
            print(f"[KICK] {player_name} kicked {target_name}: {reason}")
            
            # Notify everyone
            packet = struct.pack('>BB', 0x0d, 0xff)
            packet += pad_string(f"&e{target_name} was kicked: {reason}")
            broadcast(packet)
            
            # Kick the player
            kick_player(target_socket, reason)
        else:
            send_message(client_socket, f"&cPlayer '{target_name}' not found")
    
    elif command == '/tp':
        if len(parts) < 2:
            send_message(client_socket, "&cUsage: /tp <player> or /tp <x> <y> <z>")
            return
        
        if len(parts) == 2:
            # Teleport to another player
            target_name = parts[1]
            
            # Find target player's position (we'd need to track positions for this)
            # For now, just teleport to spawn
            send_message(client_socket, f"&aTeleporting to {target_name}...")
            teleport_player(client_socket, 64*32, 33*32, 64*32)
        
        elif len(parts) == 4:
            # Teleport to coordinates
            try:
                x = int(float(parts[1]) * 32)  # Convert to fixed-point
                y = int(float(parts[2]) * 32)
                z = int(float(parts[3]) * 32)
                
                # Validate coordinates
                if 0 <= x < X*32 and 0 <= y < Y*32 and 0 <= z < Z*32:
                    send_message(client_socket, f"&aTeleporting to {x//32}, {y//32}, {z//32}...")
                    teleport_player(client_socket, x, y, z)
                    print(f"[TP] {player_name} teleported to ({x//32}, {y//32}, {z//32})")
                else:
                    send_message(client_socket, "&cCoordinates out of bounds!")
            except ValueError:
                send_message(client_socket, "&cInvalid coordinates!")
        else:
            send_message(client_socket, "&cUsage: /tp <player> or /tp <x> <y> <z>")
    
    elif command == '/help':
        send_message(client_socket, "&eAvailable commands:")
        send_message(client_socket, "&a/kick <player> [reason]")
        send_message(client_socket, "&a/tp <player> or /tp <x> <y> <z>")
        send_message(client_socket, "&a/help - Show this message")
    
    else:
        send_message(client_socket, f"&cUnknown command: {command}")

def handle_client(client_socket, address):
    global next_player_id, world_data
    
    player_name = "Unknown"
    player_id = -1
    
    try:
        print(f"[CONNECT] New connection from {address}")
        
        # === HANDSHAKE (0x00) ===
        # Client sends: [0x00][protocol_version][username*64][verify_key*64][unused]
        handshake_id = recv_exact(client_socket, 1)[0]
        if handshake_id != 0x00:
            print(f"[ERROR] Bad handshake ID: {handshake_id}")
            return
        
        protocol_version = recv_exact(client_socket, 1)[0]
        player_name = recv_exact(client_socket, 64).decode('ascii').strip()
        verify_key = recv_exact(client_socket, 64)
        unused_byte = recv_exact(client_socket, 1)
        
        print(f"[LOGIN] Player: {player_name}, Protocol: {protocol_version}")
        
        # Assign player ID
        with clients_lock:
            player_id = next_player_id
            next_player_id = (next_player_id + 1) % 128
            if next_player_id == 255:  # Skip 255 (reserved for self)
                next_player_id = 0
            clients[client_socket] = (player_name, player_id)
        
        # === SERVER IDENTIFICATION (0x00) ===
        # [0x00][protocol_version][server_name*64][server_motd*64][user_type]
        packet = struct.pack('BB', 0x00, 0x07)
        packet += pad_string("Classic Server")
        packet += pad_string("Welcome!")
        packet += struct.pack('B', 0x00)  # User type: normal
        client_socket.sendall(packet)
        
        # === LEVEL INITIALIZE (0x02) ===
        client_socket.sendall(struct.pack('B', 0x02))
        
        # === LEVEL DATA (0x03) ===
        # Compress world data
        level_data = struct.pack('>I', len(world_data)) + world_data
        compressed = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed, mode='wb') as gz:
            gz.write(level_data)
        compressed_data = compressed.getvalue()
        
        # Send in chunks
        chunk_size = 1024
        for i in range(0, len(compressed_data), chunk_size):
            chunk = compressed_data[i:i+chunk_size]
            percent = int((i + len(chunk)) * 100 / len(compressed_data))
            
            # [0x03][chunk_length:short][chunk_data*1024][percent_complete]
            packet = struct.pack('>BH', 0x03, len(chunk))
            packet += chunk.ljust(1024, b'\x00')
            packet += struct.pack('B', percent)
            client_socket.sendall(packet)
        
        # === LEVEL FINALIZE (0x04) ===
        # [0x04][x:short][y:short][z:short]
        packet = struct.pack('>Bhhh', 0x04, X, Y, Z)
        client_socket.sendall(packet)
        
        # === SPAWN PLAYER (0x07) ===
        # Spawn self (player_id = -1 for self)
        spawn_x, spawn_y, spawn_z = 64 * 32, 33 * 32, 64 * 32
        packet = struct.pack('>Bb', 0x07, -1)
        packet += pad_string(player_name)
        packet += struct.pack('>hhhBB', spawn_x, spawn_y, spawn_z, 0, 0)
        client_socket.sendall(packet)
        
        # Spawn existing players for new player
        with clients_lock:
            for sock, (name, pid) in clients.items():
                if sock != client_socket:
                    # Tell new player about existing player
                    packet = struct.pack('>Bb', 0x07, pid)
                    packet += pad_string(name)
                    packet += struct.pack('>hhhBB', spawn_x, spawn_y, spawn_z, 0, 0)
                    client_socket.sendall(packet)
                    
                    # Tell existing player about new player
                    packet = struct.pack('>Bb', 0x07, player_id)
                    packet += pad_string(player_name)
                    packet += struct.pack('>hhhBB', spawn_x, spawn_y, spawn_z, 0, 0)
                    try:
                        sock.sendall(packet)
                    except:
                        pass
                        
        mpacket = struct.pack('>BB', 0x0d, 0xff)
        mpacket += pad_string(f"&e{player_name} has joined the game")
        broadcast(mpacket)
        print(f"[SPAWN] {player_name} joined (ID: {player_id})")
        
        # === MAIN GAME LOOP ===
        while True:
            packet_id = recv_exact(client_socket, 1)[0]
            
            if packet_id == 0x05:  # Set Block
                # [0x05][x:short][y:short][z:short][mode:byte][block_type:byte]
                x = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                y = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                z = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                mode = recv_exact(client_socket, 1)[0]
                block_type = recv_exact(client_socket, 1)[0]
                
                # Validate coordinates
                if 0 <= x < X and 0 <= y < Y and 0 <= z < Z:
                    index = (y * Z + z) * X + x
                    world_data[index] = block_type if mode == 1 else 0
                    
                    # Broadcast to all clients
                    # [0x06][x:short][y:short][z:short][block_type:byte]
                    packet = struct.pack('>BhhhB', 0x06, x, y, z, world_data[index])
                    broadcast(packet)
                    
                    # Save world (async)
                    threading.Thread(target=save_world, daemon=True).start()
            
            elif packet_id == 0x08:  # Position & Orientation
                # Client: [0x08][player_id:byte][x:short][y:short][z:short][yaw:byte][pitch:byte]
                pid = recv_exact(client_socket, 1)[0]
                x = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                y = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                z = struct.unpack('>h', recv_exact(client_socket, 2))[0]
                yaw = recv_exact(client_socket, 1)[0]
                pitch = recv_exact(client_socket, 1)[0]
                
                # Broadcast with server's player ID
                # Server: [0x08][player_id:byte][x:short][y:short][z:short][yaw:byte][pitch:byte]
                packet = struct.pack('>BbhhhBB', 0x08, player_id, x, y, z, yaw, pitch)
                broadcast(packet, exclude_socket=client_socket)
            
            elif packet_id == 0x0d:  # Message
                # [0x0d][player_id:byte][message*64]
                pid = recv_exact(client_socket, 1)[0]
                message = recv_exact(client_socket, 64).decode('ascii').strip()
                
                if message:
                    # Check for commands
                    if message.startswith('/'):
                        handle_command(client_socket, player_name, message)
                    else:
                        print(f"[CHAT] <{player_name}> {message}")
                        
                        # Broadcast to all
                        # [0x0d][player_id:byte][message*64]
                        packet = struct.pack('>BB', 0x0d, 0xff)  # 0xff = system message
                        packet += pad_string(f"&f<{player_name}> {message}")
                        broadcast(packet)
            
            else:
                print(f"[WARN] Unknown packet: 0x{packet_id:02x} from {player_name}")
    
    except ConnectionError:
        print(f"[DISCONNECT] {player_name} lost connection")
    except Exception as e:
        print(f"[ERROR] {player_name}: {e}")
    finally:
        # Clean up
        with clients_lock:
            if client_socket in clients:
                del clients[client_socket]
        
        # Notify others of disconnect
        # [0x0c][player_id:byte]
        if player_id >= 0:
            packet = struct.pack('>Bb', 0x0c, player_id)
            broadcast(packet)
            mpacket = struct.pack('>BB', 0x0d, 0xff)
            mpacket += pad_string(f"&e{player_name} has left the game")
            broadcast(mpacket)
            print(f"[LEAVE] {player_name} left the game")
        
        try:
            client_socket.close()
        except:
            pass

def main():
    global world_data
    
    # Load or create world
    if os.path.exists("world.dat"):
        try:
            with lzma.open("world.dat", "rb") as f:
                world_data = bytearray(f.read())
            print(f"[SERVER] Loaded world ({len(world_data)} bytes)")
        except Exception as e:
            print(f"[ERROR] Failed to load world: {e}")
            print("[SERVER] Generating new world...")
            world_data = bytearray(X * Y * Z)
            # Fill bottom half with grass
            for i in range(len(world_data) // 2):
                world_data[i] = 2
            save_world()
    else:
        print("[SERVER] Generating new world...")
        world_data = bytearray(X * Y * Z)
        for i in range(len(world_data) // 2):
            world_data[i] = 2
        save_world()
    
    # Start server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', 25565))
    server.listen(5)
    
    print("[SERVER] Minecraft Classic 0.30 Server")
    print("[SERVER] Listening on port 25565...")
    
    try:
        while True:
            client_sock, client_addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(client_sock, client_addr), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down...")
        save_world()
        server.close()

if __name__ == "__main__":
    main()