import amass
from amass.commands import CommonArgs, Arg

class Command(amass.commands.list.DjangoListCommand):
	usage = CommonArgs("""
    List available gateway types
    """, [], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__
