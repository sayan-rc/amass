import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Delete gateway error type
			""",
			[
				Arg("gateway type", "Type of gateway"),
				Arg("error", "Name of gateway error type to remove"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[source_gateway_type, source_gateway_error] = self.get_django_models(
			config, "SourceGatewayType", "SourceGatewayError")
		gw_type = None
		try:
			gw_type = source_gateway_type.objects.get(type=arg_vals["gateway type"])
		except:
			amass.abort("Gateway type '%s' does not exist" % arg_vals["gateway type"])

		try:
			e = source_gateway_error.objects.get(gateway_type=gw_type, error=arg_vals["error"])
			e.delete()
			print "Gateway error type '%s' for gateway '%s' sucessfully deleted" % (arg_vals["error"], arg_vals["gateway type"])
		except Exception as e:
			amass.abort("Problem deleting gateway error type '%s' for gateway '%s': %s" % (arg_vals["error"], arg_vals["gateway type"], str(e)))