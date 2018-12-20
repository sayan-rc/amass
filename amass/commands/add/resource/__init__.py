import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Add a new resource
			""",
			[
				Arg("resource", "Name of resource to add")
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[resource] = self.get_django_models(config, "Resource")

		try:
			resource.objects.create(name=arg_vals["resource"],)
			print "Resource '%s' sucessfully added" % arg_vals["resource"]
		except Exception as e:
			amass.abort("Problem adding resource '%s': %s" % (arg_vals["resource"], str(e)))
