from src.util.logger import log
from src.util.server import Server

if __name__ == '__main__':
    try:
        server = Server()
        server.run()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log(f'Error: {e}', "ERROR")
    finally:
        log('Shutting down...')