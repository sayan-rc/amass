import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Add a new source resource.  E.g., if CIPRES refers to Comet as 'comet' and
			Inca refers to Comet as 'sdsc-comet'
			""",
			[
				Arg("resource", "Name of AMASS resource"),
				Arg("source", "Name of source data -- e.g., inca, gateway.cipres"),
				Arg("source resource", "Name of source resource"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[resource, source_resource] = self.get_django_models(config, "Resource", "SourceResource")
		amass_resource = None
		try:
			amass_resource = resource.objects.get(name=arg_vals["resource"])
		except Exception as e:
			amass.abort("'%s' is not a known AMASS resource" % arg_vals["resource"])

		try:
			source_resource.objects.create(resource=amass_resource, source=arg_vals["source"], source_name=arg_vals["source resource"])
			print "Successfully added source resource '%s' for source '%s'" % (arg_vals["source resource"], arg_vals["source"])
		except Exception as e:
			amass.abort("Problem adding source resource '%s': %s" % (arg_vals["resource"], str(e)))

