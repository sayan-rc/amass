import amass
from amass.commands import CommonArgs, Opt

class Command(amass.commands.list.DjangoListCommand):
	usage = CommonArgs("""
    List gateway configuration
    """, [], [
			Opt("gateway", "Only display config parameters for specified gateway", None)
		])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__