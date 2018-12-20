import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Delete source resource name
			""",
			[
				Arg("resource", "Name of AMASS resource"),
				Arg("source", "Name of source to remove"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[resource, source_resource] = self.get_django_models(
			config, "Resource", "SourceResource")
		amass_resource = None
		try:
			amass_resource = resource.objects.get(name=arg_vals["resource"])
		except:
			amass.abort("'%s' is not a known resource" % arg_vals["resource"])

		try:
			s_resource = source_resource.objects.get(resource=amass_resource, source=arg_vals["source"])
			s_resource.delete()
			print "Source resource for '%s' and '%s' sucessfully deleted" % (arg_vals["source"], arg_vals["resource"])
		except Exception as e:
			amass.abort("Problem deleting source resource '%s' for resource '%s': %s" % (arg_vals["resource"], arg_vals["source"], str(e)))