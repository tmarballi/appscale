import webapp2

class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Hello, Health Probe Never!')


app = webapp2.WSGIApplication([
    ('/healthprobenever', MainPage),
], debug=True)
