#!/usr/bin/env node

/**
 * @flow
 * @fomat
 */

/**
 * External dependencies
 */
import debugLib from 'debug';

/**
 * Internal dependencies
 */
import command from 'lib/cli/command';
import { searchAndReplace } from 'lib/search-and-replace';

const debug = debugLib( '@automattic/vip:bin:vip-search-replace' );

// Command examples
const examples = [
	// `search-replace` flag
	{
		usage: 'vip search-replace <file.sql> --search-replace="from,to"',
		description: 'Replace instances of <from> with <to> in the provided <file.sql>\n' +
		'       * Ensure there are no spaces between your search-replace parameters',
	},
	// `in-place` flag
	{
		usage: 'vip search-replace <file.sql> --search-replace="from,to" --in-place',
		description: 'Perform Search and Replace explicitly on the provided input <file.sql> file',
	},
	// `output` flag
	{
		usage: 'vip search-replace <file.sql> --search-replace="from,to" --output="<output.sql>"',
		description: 'Search and Replace to the specified output <output.sql> file\n' +
		'       * Has no effect when the `in-place` flag is used',
	},
];

command( {
	requiredArgs: 1,
} )
	.option( 'search-replace', 'Specify the <from> and <to> pairs to be replaced' )
	.option( 'in-place', 'Perform the search and replace explicitly on the input file' )
	.option( 'output', 'Specify the replacement output file for Search and Replace' )
	.examples( examples )
	.argv( process.argv, async ( arg, opt ) => {
		// TODO: tracks event for usage of this command stand alone
		const { searchReplace, inPlace, output } = opt;

		debug( 'Args: ', arg, 'searchReplace: ', searchReplace );

		const filename = arg[ 0 ];
		if ( ! arg && ! filename ) {
			console.error( 'You must pass in a filename' );
			process.exit( 1 );
		}

		if ( ! searchReplace || ! searchReplace.length ) {
			console.error( 'You must provide a pair of strings (separated by comma) such as original,replacement' );
			process.exit( 1 );
		}

		const isImport = false;
		await searchAndReplace( filename, searchReplace, { isImport, inPlace, output } );
	} );
