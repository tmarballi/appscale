import webapp2

class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Hello, Health Check Never!')


app = webapp2.WSGIApplication([
    ('/healthchecknever', MainPage),
], debug=True)
