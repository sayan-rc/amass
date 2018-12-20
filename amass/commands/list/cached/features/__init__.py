import amass
from amass.commands import CommonArgs, Opt
import sys

class Command(amass.commands.Command):
	usage = CommonArgs(
		"""
		List the cached features
		""",
		[],
		[Opt("names", "Display the names for each feature in vector", "False")]
	)

	def __init__(self):
		amass.commands.Command.__init__(self)

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		features = amass.features.Features(config)
		table = features.list_cached_features()
		if not self.is_arg_true(arg_vals["names"]):
			amass.print_table(
				["FEATURES", "GATEWAY", "COUNT", "START DATE", "END DATE"],
				table)
			sys.exit(0)

		new_table = []
		for row in table:
			feature_names = features.list_feature_names(row[0], row[1])
			for i, feature_name in enumerate(feature_names):
				new_table.append([row[0], row[1], str(i), feature_name])
		amass.print_table(["FEATURES", "GATEWAY", "INDEX", "NAME"], new_table)

