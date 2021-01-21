/**
 * @flow
 * @format
 */

/**
 * External dependencies
 */
import readline from 'readline';
import fs from 'fs';
import chalk from 'chalk';

/**
 * Internal dependencies
 */

function openFile( filename, flags = 'r', mode = 666 ) {
	return new Promise( ( resolve, reject ) => {
		fs.open( filename, flags, mode, ( err, fd ) => {
			if ( err ) {
				return reject( err );
			}
			resolve( fd );
		} );
	} );
}

export async function getReadInterface( filename: string ) {
	let fd;
	try {
		fd = await openFile( filename );
	} catch ( e ) {
		console.log( chalk.red( 'Error: ' ) + 'The file at the provided path is either missing or not readable.' );
		console.log( 'Please check the input and try again.' );
		process.exit( 1 );
	}

	return readline.createInterface( {
		input: fs.createReadStream( '', { fd } ),
		output: null,
		console: false,
	} );
}

export function validateLineByLine( readInterface, functions: array ) {
	readInterface.on( 'line', function( line ) {
		if ( lineNum % 500 === 0 ) {
			log( `Reading line ${ lineNum } ` );
		}

		functions.map( validationObject => {

		} );

		const checkValues: any = Object.values( checks );
		checkValues.forEach( ( check: CheckType ) => {
			const results = line.match( check.matcher );
			if ( results ) {
				check.results.push( check.matchHandler( lineNum, results ) );
			}
		} );

		lineNum += 1;
	} );
}
