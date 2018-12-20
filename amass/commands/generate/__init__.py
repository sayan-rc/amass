import amass


class Command(amass.commands.Command):
	is_command = False

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__