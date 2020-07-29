# user input expected = connection.market.live
# connection.snp.live should give ('127.0.0.1', 1300, 1)
# connection.snp.paper should give ('127.0.0.1', 1301, 1)
# connection.nse.live should give ('127.0.0.1', 3000, 1)
# connection.nse.paper should give ('127.0.0.1', 3001, 1)

class Snp:
    def __init__(self):
        self.live = Live()
        print("I am in SNP")

class Nse:
    def __init__(self):
        self.live = Live()
        print("I am in NSE")

class Live:
        print("I am in Live")

class Connection:

    # Class attributes
    ip = '127.0.0.1'
    cid = 1 
    port_dict = {
                    ('SNP', True): 1300,
                    ('SNP', False): 1301,
                    ('NSE', True): 3000, 
                    ('NSE', False): 3001
                }
    def __init__(self):
        self.snp = Snp
        self.nse = Nse


if __name__ == "__main__":
    
    connection = Connection()
    print(connection.snp)
