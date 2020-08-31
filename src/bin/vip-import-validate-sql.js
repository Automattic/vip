#!/usr/bin/env node
// @flow

/**
 * External dependencies
 */
import readline from 'readline';
import fs from 'fs';
import chalk from 'chalk';
import { stdout as log } from 'single-line-log';

/**
 * Internal dependencies
 */
import command from 'lib/cli/command';
import { trackEvent } from 'lib/tracker';

let problemsFound = 0;
let lineNum = 1;

const errorCheckFormatter = check => {
	if ( check.results.length > 0 ) {
		problemsFound += 1;
		console.error( chalk.red( 'Error:' ), `${ check.message } on line(s) ${ check.results.join( ',' ) }.` );
		console.error( chalk.yellow( 'Recommendation:' ), `${ check.recommendation }` );
	} else {
		console.log( `✅ ${ check.message } was found ${ check.results.length } times.` );
	}
};

const requiredCheckFormatter = ( check, type ) => {
	if ( check.results.length > 0 ) {
		console.log( `✅ ${ check.message } was found ${ check.results.length } times.` );
		if ( type === 'createTable' ) {
			checkTablePrefixes( check.results );
		}
	} else {
		problemsFound += 1;
		console.error( chalk.red( 'Error:' ), `${ check.message } was not found.` );
		console.error( chalk.yellow( 'Recommendation:' ), `${ check.recommendation }` );
	}
};

const infoCheckFormatter = check => {
	check.results.forEach( item => {
		console.log( item );
	} );
};

const checks = {
	useDB: {
		matcher: /^use\s/i,
		matchHandler: lineNumber => lineNumber,
		outputFormatter: errorCheckFormatter,
		results: [],
		message: 'USE statement',
		excerpt: '\'USE\' statement should not be present (case-insensitive, at beginning of line)',
		recommendation: 'Remove these lines',
	},
	createDB: {
		matcher: /^CREATE DATABASE/i,
		matchHandler: lineNumber => lineNumber,
		outputFormatter: errorCheckFormatter,
		results: [],
		message: 'CREATE DATABASE statement',
		excerpt: '\'CREATE DATABASE\' statement should not  be present (case-insensitive)',
		recommendation: 'Remove these lines',
	},
	dropDB: {
		matcher: /^DROP DATABASE/i,
		matchHandler: lineNumber => lineNumber,
		outputFormatter: errorCheckFormatter,
		results: [],
		message: 'DROP DATABASE statement',
		excerpt: '\'DROP DATABASE\' should not be present (case-insensitive)',
		recommendation: 'Remove these lines',
	},
	alterUser: {
		matcher: /^(ALTER USER|SET PASSWORD)/i,
		matchHandler: lineNumber => lineNumber,
		outputFormatter: errorCheckFormatter,
		results: [],
		message: 'ALTER USER statement',
		excerpt: '\'ALTER USER\' should not be present (case-insensitive)',
		recommendation: 'Remove these lines',
	},
	dropTable: {
		matcher: /^DROP TABLE IF EXISTS `?([a-z0-9_]*)/i,
		matchHandler: ( lineNumber, results ) => results [ 1 ],
		outputFormatter: requiredCheckFormatter,
		results: [],
		message: 'DROP TABLE',
		excerpt: '\'DROP TABLE IF EXISTS\' should be present (case-insensitive)',
		recommendation: 'Check import settings to include DROP TABLE statements',
	},
	createTable: {
		matcher: /^CREATE TABLE `?([a-z0-9_]*)/i,
		matchHandler: ( lineNumber, results ) => results [ 1 ],
		outputFormatter: requiredCheckFormatter,
		results: [],
		message: 'CREATE TABLE',
		excerpt: '\'CREATE TABLE\' should be present (case-insensitive)',
		recommendation: 'Check import settings to include CREATE TABLE statements',
	},
	autoIncrement: {
		matcher: /^`?([a-z0-9_]*)`?(?=.*AUTO_INCREMENT,.*)(?=.*,)/i,
		matchHandler: ( lineNumber, results ) => results [ 1 ],
		outputFormatter: requiredCheckFormatter,
		results: [],
		message: 'AUTO_INCREMENT column',
		excerpt: '\'CREATE TABLE\' statements should include a column with the AUTO_INCREMENT option',
		recommendation: 'Check import settings to ensure CREATE TABLE statements contain a column with AUTO_INCREMENT',
	},
	siteHomeUrl: {
		matcher: '\'(siteurl|home)\',\\s?\'(.*?)\'',
		matchHandler: ( lineNumber, results ) => results [ 0 ],
		outputFormatter: infoCheckFormatter,
		results: [],
		message: 'Siteurl/home matches',
		excerpt: 'Siteurl/home options',
		recommendation: '',
	},
};

function checkTablePrefixes( tables ) {
	const wpTables = [], notWPTables = [], wpMultisiteTables = [];
	tables.forEach( tableName => {
		if ( tableName.match( /^wp_(\d+_)/ ) ) {
			wpMultisiteTables.push( tableName );
		} else if ( tableName.match( /^wp_/ ) ) {
			wpTables.push( tableName );
		} else if ( ! tableName.match( /^wp_/ ) ) {
			notWPTables.push( tableName );
		}
	} );
	if ( wpTables.length > 0 ) {
		console.log( ` - wp_ prefix tables found: ${ wpTables.length } ` );
	}
	if ( notWPTables.length > 0 ) {
		problemsFound += 1;
		console.error( chalk.red( 'Error:' ), `tables without wp_ prefix found: ${ notWPTables.join( ',' ) } ` );
	}
	if ( wpMultisiteTables.length > 0 ) {
		console.log( ` - wp_n_ prefix tables found: ${ wpMultisiteTables.length } ` );
	}
}

command( {
	requiredArgs: 1,
} )
	.example( 'vip import validate sql <file>', 'Run the import validation against file' )
	.argv( process.argv, async arg => {
		if ( ! arg && ! arg[ 0 ] ) {
			console.error( 'You must pass in a filename' );
			process.exit( 1 );
		}

		await trackEvent( 'import_validate_sql_command_execute' );

		const readInterface = readline.createInterface( {
			input: fs.createReadStream( arg[ 0 ] ),
			output: null,
			console: false,
		} );

		readInterface.on( 'line', function( line ) {
			if ( lineNum % 500 === 0 ) {
				log( `Reading line ${ lineNum } ` );
			}

			Object.values( checks ).forEach( check => {
				const results = line.match( check.matcher );
				if ( results ) {
					check.results.push( check.matchHandler( lineNum, results ) );
				}
			} );
			lineNum += 1;
		} );

		readInterface.on( 'close', async function() {
			log( `Finished processing ${ lineNum } lines.` );
			console.log( '\n' );
			const errorSummary = {};
			for ( const [ type, check ] of Object.entries( checks ) ) {
				check.outputFormatter( check, type );
				console.log( '' );

				// Change `type` to snake_case for Tracks events
				const typeToSnakeCase = type.replace( /([A-Z])/, '_$1' ).toLowerCase();

				errorSummary[ typeToSnakeCase ] = check.results.length;
			}
			errorSummary.problems_found = problemsFound;

			if ( checks.createTable.results.length > checks.autoIncrement.results.length ) {
				const missingAutoIncrement = checks.createTable.results.length - checks.autoIncrement.results.length;
				problemsFound++;
				console.error( chalk.red( 'Error:' ), `${ missingAutoIncrement } table(s) appear to be missing an AUTO_INCREMENT column` );
			}

			if ( problemsFound > 0 ) {
				console.error( `Total of ${ chalk.red( problemsFound ) } errors found` );
			} else {
				console.log( '✅ Your database file looks good.  You can now submit for import, see here for more details: ' +
				'https://wpvip.com/documentation/vip-go/migrating-and-importing-content/#submitting-the-database' );
			}

			await trackEvent( 'import_validate_sql_command_success', errorSummary );
		} );
	} );
