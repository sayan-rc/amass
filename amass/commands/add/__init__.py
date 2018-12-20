import amass


class DjangoAddCommand(amass.commands.DjangoCommand):
	"""
	Converts list commands that interact with Django into Django commands
	"""

	def get_add_django_class(self, config):
		# trim off "amass.commands.add"
		add_command = self.__module__.replace(DjangoAddCommand.__module__, "").split(".")
		classname = "".join([cmd.capitalize() for cmd in add_command])
		[addclass] = self.get_django_models(config, classname)
		return addclass

class Command(amass.commands.Command):
	is_command = False

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__