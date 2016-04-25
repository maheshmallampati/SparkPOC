package com.mcd.gdw.daas.util;

import java.security.InvalidKeyException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class SimpleEncryptAndDecrypt {
	
	private final static String SECURITY_ALGORITHM = "DESede";
	private final static String SECRET_KEY_VALUE = "B5D362AD10972A89C7FB108FB9072575B5D362AD10972A89";
	private final static char[] HEX_ARRAY = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
	
	private static SecretKey secretKey = null;
	private static Cipher cipher = null;
	
	public SimpleEncryptAndDecrypt() {
		
		try {
			
			secretKey = new SecretKeySpec(hexStringToByteArray(SECRET_KEY_VALUE), SECURITY_ALGORITHM );
			cipher = Cipher.getInstance(SECURITY_ALGORITHM);
			
		} catch (Exception ex) {
			System.err.println("SimpleEncryptAndDecrypt initialization failed");
			System.exit(8);
		}
	}

	public byte[] encrypt(String input) throws InvalidKeyException
	                                          ,BadPaddingException
	                                          ,IllegalBlockSizeException {

		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		byte[] inputBytes = input.getBytes();
		return(cipher.doFinal(inputBytes));

	}

	public String encryptAsHexString(String input) throws InvalidKeyException
	                                                     ,BadPaddingException
	                                                     ,IllegalBlockSizeException {

		return(bytesToHex(encrypt(input)));

	}

	public String decrypt(byte[] encryptionBytes) throws InvalidKeyException
	                                                    ,BadPaddingException
	                                                    ,IllegalBlockSizeException {

		cipher.init(Cipher.DECRYPT_MODE, secretKey);
		byte[] recoveredBytes = cipher.doFinal(encryptionBytes);
		String recovered = new String(recoveredBytes);
		return(recovered);

	}

	public String decryptFromHexString(String encryptionString) throws InvalidKeyException
	                                                                  ,BadPaddingException
	                                                                  ,IllegalBlockSizeException {


		return(decrypt(hexStringToByteArray(encryptionString)));

	}
	
	public static String bytesToHex(byte[] bytes) {

		 char[] hexChars = new char[bytes.length * 2];
		 int v;

		 for ( int j = 0; j < bytes.length; j++ ) {
			 v = bytes[j] & 0xFF;
			 hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			 hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		 }

		 return(new String(hexChars));

	}

	public static byte[] hexStringToByteArray(String s) {
		
		int len = s.length();

		byte[] data = new byte[len / 2];

		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i+1), 16));
		}

		return(data);
	}	
}