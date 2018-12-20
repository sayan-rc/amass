import amass
from amass.commands import CommonArgs, Opt
import sys

class Command(amass.commands.Command):
	usage = CommonArgs(
		"""
		List the cached sources
		""",
		[],
		[]
	)

	def __init__(self):
		amass.commands.Command.__init__(self)

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		features = amass.features.Features(config)
		table = features.list_cached_sources()
		amass.print_table(["SOURCE", "COUNT", "START DATE", "END DATE"], table)


