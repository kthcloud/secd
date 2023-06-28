from src import server, setup
from src.logger import log

if __name__ == '__main__':
    try:
        log('Loading settings...')
        setup.load_settings()

        log("Starting up...")
        server.run()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log(f'Error: {e}', "ERROR")

    finally:
        log('Shutting down...')
