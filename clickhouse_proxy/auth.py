import struct

class Auth:
    @staticmethod
    def is_ipv6(ip: str) -> bool:
        return ip.find(':') > -1


    def ip_to_chunks(ip:str) -> bytes:
        if Auth.is_ipv6(ip):
            separator = ':'
            chunks = [None] * 8
            bytes_per_chunk = 2
        else:
            separator = '.'
            chunks = [None] * 4
            bytes_per_chunk = 1
        
        str_chunks = ip.split(separator)
        src_chunk_id = 0
        for id in range(0, len(chunks) - 1):
            chunk = str_chunks[src_chunk_id]
            if chunk:
                chunks[id] = chunk
                src_chunk_id += 1
            else:
                chunks[id] = '0'
        chunks[-1] = str_chunks[-1] or 0
        fmt = f'{{:0>{bytes_per_chunk * 2}}}'
        return [fmt.format(c) for c in chunks]


    @staticmethod
    def addressInNetwork(ip, net):
        "Is an address in a network"
        # Get byte list for IP address
        bs_ipaddr = socket.inet_aton('127.0.0.1')
        # Unpack struct. ! stands for Network Address, which means Big Endian (natural order of bytes), L is for Long
        i_ip = struct.unpack('!L',bs_ipaddr)[0]
        netaddr,bits = net.split('/')
        bs_netaddr=socket.inet_aton(netaddr)
        i_net = struct.unpack('!L',bs_netaddr)[0]
        # Compute significant bitmask (each bit is 1 except for the last, insignificant ones)
        bits=32-int(bits)
        bitmask=0xFFFFFFFF ^ ((1<<bits) - 1)

        # Check whether significant bits match
        return i_ip & bitmask == i_net & bitmask


    def authorise(self, params, remote_addr):
        user = params.get('user', 'default')

        if user not in config.users:
            return f"User '{user}' is not authorised to access the ClickHouse ODBC Proxy"

        authorized = False
        for ip in config.users[user].get('ips', []):
            if self.addressInNetwork(remote_addr, ip):
                authorized = True
                break
        if not authorized:
            return f"User '{user}' is not authorised to access the ClickHouse ODBC Proxy from IP {remote_addr}"

        return None


if __name__=='__main__':
    auth = Auth()
    assert Auth.is_ipv6('1232:ade2::0') == True
    assert Auth.is_ipv6('127.0.0.1') == False
    #assert Auth.ip_to_chunks('1232:ade2::20') == ['1232', 'ade2', '0', '0', '0', '0', '0', '20']
    print(Auth.ip_to_chunks('1232:ade2::20'))
