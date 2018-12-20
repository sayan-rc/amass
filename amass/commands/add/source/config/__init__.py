import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Add a new feature source configuration parameter
			""",
			[
				Arg("source", "Name of source to add config parameter to"),
				Arg("name", "Name of source config parameter"),
				Arg("value", "Value of source config parameter"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[source_config] = self.get_django_models(config, "SourceConfig")

		try:
			source_config.objects.create(source=arg_vals["source"], name=arg_vals["name"], value=arg_vals["value"])
			print "Config parameter '%s' sucessfully added for source '%s'" % (arg_vals["name"], arg_vals["source"])
		except Exception as e:
			amass.abort("Problem adding config '%s' for source '%s': %s" % (arg_vals["name"], arg_vals["source"], str(e)))
