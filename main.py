from src import server, setup

if __name__ == '__main__':
    try:
        print('Loading settings...')
        setup.load_settings()

        print('Starting up...')
        server.run()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f'Error: {e}')
    finally:
        print('Shutting down...')