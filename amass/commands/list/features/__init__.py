import amass
import amass.features
from amass.commands import CommonArgs, Arg


class Command(amass.commands.Command):
	usage = CommonArgs(
		"""
		List available defined features
		""",
		[
		],
		[
		]
	)

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		self.parse_args(args)
		features = amass.features.Features(config)
		table = features.list_features()
		amass.print_table(["FEATURE SET NAME", "FEATURE NAMES"], table)


