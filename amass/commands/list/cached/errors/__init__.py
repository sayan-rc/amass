import amass
from amass.commands import CommonArgs, Arg, Opt

class Command(amass.commands.Command):
	usage = CommonArgs(
		"""
		List all errors present in cached features
		""",
		[
			Arg("features", "Name of defined feature set"),
			Arg("gateway", "Name of gateway")
		],
		[
			Opt("printerror", "Print out all errors matching specified type", "")
		]
	)

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		features = amass.features.Features(config)
		table, unmatched_errors = features.list_cached_feature_errors(
			arg_vals["features"],arg_vals["gateway"], arg_vals["printerror"])

		if arg_vals["printerror"] != "":
			amass.print_table(["ERROR"], table)
		else:
			amass.print_table(["ERROR TYPE", "ERROR", "COUNT"], table)
			if len(unmatched_errors) > 0:
				print "\nThe following errors are unmatched:"
				for error in unmatched_errors:
					print error
