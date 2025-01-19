import java.io.File;
import java.io.IOException;
import java.util.Random;

public class ClientMain {
	
	public static void main(String[] args) throws Exception{
		
		final int cport = 12345;
		int timeout = Config.timeout;
		
		// this client expects a 'downloads' folder in the current directory; all files loaded from the store will be stored in this folder
		File downloadFolder = new File("downloads");
		if (!downloadFolder.exists())
			if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create 'downloads' folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");
		
		// this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
		File uploadFolder = new File("to_store");
		if (!uploadFolder.exists())
			if (!uploadFolder.mkdir()) throw new RuntimeException("Cannot create 'to_store' folder (folder absolute path: " + uploadFolder.getAbsolutePath() + ")");
		
		// launch a single client
		testTest1Client(cport, timeout, downloadFolder, uploadFolder);
//		testClient(cport, timeout, downloadFolder, uploadFolder);
//
//		// launch a number of concurrent clients, each doing the same operations
//		for (int i = 0; i < 10; i++) {
//			new Thread() {
//				public void run() {
//					test2Client(cport, timeout, downloadFolder, uploadFolder);
//				}
//			}.start();
//		}
//		for (int i = 0;i<2;i++){
//			new Thread() {
//				@Override
//				public void run() {
//					testListClient(cport, timeout, downloadFolder, uploadFolder);
//				}
//			}.start();
//		}
//		for (int i = 0;i<5;i++){
//			new Thread() {
//				@Override
//				public void run() {
//					testTest2Client(cport, timeout, downloadFolder, uploadFolder);
//				}
//			}.start();
//		}
	}
	
	public static void test2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;

		try {
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
			client.connect();
			Random random = new Random(System.currentTimeMillis() * System.nanoTime());
			
			File fileList[] = uploadFolder.listFiles();
			for (int i=0; i<fileList.length/2; i++) {
				File fileToStore = fileList[random.nextInt(fileList.length)];
				try {					
					client.store(fileToStore);
				} catch (Exception e) {
					System.out.println("Error storing file " + fileToStore);
					e.printStackTrace();
				}
			}
			
			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			for (int i = 0; i < list.length/4; i++) {
				String fileToRemove = list[random.nextInt(list.length)];
				try {
					client.remove(fileToRemove);
				} catch (Exception e) {
					System.out.println("Error remove file " + fileToRemove);
					e.printStackTrace();
				}
			}
			
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}
	
	public static void testClient(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;
		
		try {
			
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
		
			try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
			// store first file in the to_store folder twice, then store second file in the to_store folder once
			File fileList[] = uploadFolder.listFiles();
			if (fileList.length > 0) {
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }				
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			}
			if (fileList.length > 1) {
				try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
			}

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
			if (list != null && list.length > 0)
				try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

	public static void testTest1Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;

		try {

			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

			try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }

			try { list(client); } catch(IOException e) { e.printStackTrace(); }

			// store first file in the to_store folder twice, then store second file in the to_store folder once
			File fileList[] = uploadFolder.listFiles();
			if (fileList.length > 0) {
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			}

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

			if (list != null){
				try { client.load(list[0], downloadFolder); } catch(IOException e) { e.printStackTrace(); }
				try { client.wrongLoad(list[0], 2);} catch(IOException e) { e.printStackTrace(); }
			}

			if (list != null)
				for (String filename : list)
					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
			if (list != null && list.length > 0)
				try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }
			list = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			assert list.length == 0;
			if (fileList.length > 0) {
				try { client.wrongStore(fileList[0].getName(), "12345".getBytes()); } catch(IOException e) { e.printStackTrace(); }
			}
			try { client.load(fileList[1].getName(), downloadFolder); } catch(IOException e) { e.printStackTrace(); }
			try { client.remove(fileList[1].getName()); } catch(IOException e) { e.printStackTrace(); }

		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

	public static void testTest2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;

		try {

			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

			try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }

			File fileList[] = uploadFolder.listFiles();

			if (fileList.length > 0) {
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			}
			Thread.sleep(1000);
			try { client.load(fileList[0].getName(), downloadFolder); } catch(IOException e) { e.printStackTrace(); }
			Thread.sleep(1000);
			try { client.remove(fileList[0].getName()); } catch(IOException e) { e.printStackTrace(); }


		} catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}
	public static void testListClient(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;

		try {

			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

			try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }

			try { list(client); } catch(IOException e) { e.printStackTrace(); }

		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

		public static String[] list(Client client) throws IOException, NotEnoughDstoresException {
		System.out.println("Retrieving list of files...");
		String list[] = client.list();
		
		System.out.println("Ok, " + list.length + " files:");
		int i = 0; 
		for (String filename : list)
			System.out.println("[" + i++ + "] " + filename);
		
		return list;
	}
	
}
