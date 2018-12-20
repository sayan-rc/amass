import amass
from amass.commands import CommonArgs, Arg

class Command(amass.commands.list.DjangoListCommand):
	usage = CommonArgs("""
    List resources
    """, [], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__