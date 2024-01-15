from litestar import Controller, get
from litestar.response import Template


class BokehTestController(Controller):
    
    path = "/bokehtest"
    @get(["/"], sync_to_thread=False)
    def index() -> Template:
        context = {'title':'bokeh test'}
        return Template(
            template_name='bokehtest.html', context=context
        )