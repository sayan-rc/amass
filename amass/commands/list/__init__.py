import amass

class DjangoListCommand(amass.commands.DjangoCommand):
	"""
	Converts list commands that interact with Django into Django commands
	"""

	def run(self, config, args):
		self.parse_args(args)
		# trim off "amass.commands.list"
		list_command = self.__module__.replace(DjangoListCommand.__module__, "").split(".")
		# capitalize and join to make class name of django object
		classname = "".join([cmd.capitalize() for cmd in list_command])
		[listclass] = self.get_django_models(config, classname)
		rows = []
		for listobject in listclass.objects.all():
			rows.append(listobject.to_array())
		amass.print_table(listclass.headers(), rows)

class Command(amass.commands.Command):
	is_command = False

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__
