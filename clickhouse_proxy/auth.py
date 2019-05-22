from clickhouse_proxy.config import config

def is_ipv6(ip: str) -> bool:
    return ip.find(':') > -1


def ip_to_bits(ip:str) -> bytes:
    # See what kind of IP this is and set parsing parameters
    if is_ipv6(ip):
        separator = ':'
        chunks = [None] * 8
        bytes_per_chunk = 2
        base = 16
    else:
        separator = '.'
        chunks = [None] * 4
        bytes_per_chunk = 1
        base = 10
    
    # tokenize
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
    # fmt is just for fancy viewing
    #fmt = f'{{:0>{bytes_per_chunk * 2}}}'
    #fancy_viewing = [fmt.format(c) for c in chunks]
    int_chunks = [int(c, base) for c in chunks]
    bit_ip = 0
    for chunk in int_chunks:
        bit_ip = bit_ip << (8 * bytes_per_chunk)
        bit_ip = bit_ip | chunk
    return bit_ip


def addressInNetwork(ip, net):
    """Is an address in a network
    """
    # If IP and net mask are not compatible, return False
    if is_ipv6(ip) != is_ipv6(net):
        return False

    # Convert ips to int
    i_ip = ip_to_bits(ip)
    total_bits = 32 if not is_ipv6(ip) else 128

    netparts = net.split('/')
    netaddr = netparts[0]
    bits = int(netparts[1]) if len(netparts)==2 else total_bits
    i_net = ip_to_bits(netaddr)

    # get a mask of bits of "1" of the length of total_bits
    mask_all = (2 << total_bits) - 1
    # Compute significant bitmask (each bit is 1 except for the last, insignificant ones)
    bits = total_bits - int(bits)
    bitmask = mask_all ^ ((1<<bits) - 1)

    # Check whether significant bits match
    return i_ip & bitmask == i_net & bitmask


def authorize(params, remote_addr):
    user = params.get('user', 'default')

    if user not in config.users:
        return f"User '{user}' is not authorised to access the ClickHouse ODBC Proxy"

    authorized = False
    for ip in config.users[user].get('ips', []):
        if addressInNetwork(remote_addr, ip):
            authorized = True
            break
    if not authorized:
        return f"User '{user}' is not authorised to access the ClickHouse ODBC Proxy from IP {remote_addr}"

    return None


if __name__=='__main__':
    # TODO: need more tests maybe
    assert is_ipv6('1232:ade2::0') == True
    assert is_ipv6('127.0.0.1') == False
    assert addressInNetwork('192.0.0.0.129', '192.0.0.0.128/31')

