package org.mappinganalysis.model.functions.blocking.lsh.utils;

import scala.collection.generic.BitOperations.Int;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;

/**
 * Utility class for calculating hash values.
 * 
 * @author mfranke
 */
public class HashUtils {
	
	private static final String MD5 = "MD5";
	private static final String SHA = "SHA";
	
	private HashUtils(){
		throw new RuntimeException();
	}
	
	/**
	 * Calculates the MD5 hash for a string input. 
	 * @param input a String value. 
	 * @return the {@link Int} representation of the MDH5 hash value.
	 */
	public static int getMD5(String input) {
		try {
			MessageDigest md = MessageDigest.getInstance(MD5);
			byte[] messageDigest = md.digest(input.getBytes());            
			BigInteger number = new BigInteger(1, messageDigest);
			return number.intValue();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Calculates the SHA hash for a string input.
	 * @param input a String value.
	 * @return the {@link Int} representation of the SHA hash value.
	 */
	public static int getSHA(String input) {
		try {
			MessageDigest md = MessageDigest.getInstance(SHA);
			byte[] messageDigest = md.digest(input.getBytes());            
			BigInteger number = new BigInteger(1, messageDigest);
			return number.intValue();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Calculates the SHA hash for a string input.
	 * @param input a String value.
	 * @return the {@link long} representation of the SHA hash value.
	 */	
	public static long getSHALongHash(String input){
		try{
			MessageDigest md = MessageDigest.getInstance(SHA);
			byte[] messageDigest = md.digest(input.getBytes());            
			BigInteger number = new BigInteger(1, messageDigest);
			return number.longValue();
		}
		catch (NoSuchAlgorithmException e){
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Calculates the MD5 hash for a BitSet input.
	 * @param input a {@link BitSet} object.
	 * @return the {@link BitSet} representation of the MD5 hash value.
	 */
	public static int getMD5(BitSet input) {
		try {
			MessageDigest md = MessageDigest.getInstance(MD5);
			byte[] messageDigest = md.digest(input.toByteArray());            
			BigInteger number = new BigInteger(1, messageDigest);
			return number.intValue();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Calculates the SHA hash for a BitSet input.
	 * @param input a {@link BitSet} object.
	 * @return the {@link Int} representation of the SHA hash value.
	 */
	public static int getSHA(BitSet input) {
		try {
			MessageDigest md = MessageDigest.getInstance(SHA);
			byte[] messageDigest = md.digest(input.toByteArray());
			BigInteger number = new BigInteger(1, messageDigest);
			return number.intValue();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}