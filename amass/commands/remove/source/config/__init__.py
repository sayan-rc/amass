import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Delete source configuration parameter
			""",
			[
				Arg("source", "Name of feature source"),
				Arg("name", "Name of source configuration parameter to remove"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[source_config] = self.get_django_models(config, "SourceConfig")

		try:
			cfg = source_config.objects.get(source=arg_vals["source"], name=arg_vals["name"])
			cfg.delete()
			print "Configuration parameter '%s' for source '%s' sucessfully deleted" % (arg_vals["name"], arg_vals["source"])
		except Exception as e:
			amass.abort("Problem deleting configuration parameter '%s' for source '%s': %s" % (arg_vals["name"], arg_vals["source"], str(e)))