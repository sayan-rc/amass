import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Delete resource from AMASS
			""",
			[
				Arg("resource", "Name of resource to delete"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[resource] = self.get_django_models(config, "Resource")
		r = None
		try:
			r = resource.objects.get(name=arg_vals["resource"])
		except:
			amass.abort("Resource '%s' does not exist" % arg_vals["resource"])

		try:
			r.delete()
			print "Resource '%s' sucessfully deleted" % arg_vals["resource"]
		except Exception as e:
			amass.abort("Problem deleting resource '%s': %s" % (arg_vals["resource"], str(e)))