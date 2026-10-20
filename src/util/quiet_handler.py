from wsgiref.simple_server import WSGIRequestHandler

class QuietHandler(WSGIRequestHandler):
    def log_message(self, format, *args):
        # override to suppress the default access log
        pass