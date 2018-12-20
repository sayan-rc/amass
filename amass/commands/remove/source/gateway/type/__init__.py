import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Delete gateway type from AMASS
			""",
			[
				Arg("gateway type", "Name of gateway to delete"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[source_gateway_type] = self.get_django_models(config, "SourceGatewayType")
		gw_type = None
		try:
			gw_type = source_gateway_type.objects.get(type=arg_vals["gateway type"])
		except:
			amass.abort("Gateway type '%s' does not exist" % arg_vals["gateway type"])

		try:
			gw_type.delete()
			print "Gateway type '%s' sucessfully deleted" % arg_vals["gateway type"]
		except Exception as e:
			amass.abort("Problem deleting gateway type '%s': %s" % (arg_vals["gateway type"], str(e)))