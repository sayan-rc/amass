import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Add a new gateway type
			""",
			[
				Arg("type", "Name of new gateway type"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[source_gateway_type] = self.get_django_models(config, "SourceGatewayType")
		try:
			source_gateway_type.objects.create(type=arg_vals["type"])
			print "Gateway type '%s' sucessfully added" % arg_vals["type"]
		except:
			amass.abort("Problem creating gatewa type '%s'" % arg_vals["type"])