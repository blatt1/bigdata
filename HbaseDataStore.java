package com.umbc.bigdata.datastore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDataStore 
{
	
	static Log logger = LogFactory.getLog(HbaseDataStore.class);

	public static void main(String[] args) 
	{
		logger.info("Hbase DataStore started ...");

        String sConfig;
        if (args.length != 1)
        {
        	usage();
        	System.exit(0);
        }
        sConfig = args[0];
        
        File fConfig = new File(sConfig);
        
        if (!fConfig.exists())
        {
        	logger.error("Config file does not exist.");
        	System.exit(1);
        }
        
        File fInDir           = null;
        String hTableName     = "";
        boolean bCreateTableChk = false;
        try
        {
            XMLConfiguration xConfig = new XMLConfiguration(fConfig.getAbsolutePath());
            xConfig.load();
            String sInDir = xConfig.getString("InputDir");
            fInDir = new File(sInDir);
            hTableName = xConfig.getString("TableName");
            String createTableChk = xConfig.getString("CreateNewTable");
            if (createTableChk.toUpperCase().equals("TRUE"))
            {
            	bCreateTableChk = true;
            }
        }
        catch (ConfigurationException e)
        {
        	logger.error("Error reading config file.");
        	System.exit(1);
        }
        
        String sExSymbol = getSymbol(fInDir);
        System.out.println("ExChange Symbol : " + sExSymbol);
        
        File[] fileList;
        if (fInDir.exists() && fInDir.isDirectory())
        {
        	fileList = fInDir.listFiles();
        	if (bCreateTableChk)
        	{
        		try
        		{
        			createHbaseTable(hTableName);
        		}
        		catch (IOException e)
        		{
        			logger.error("Error creating hbase table.");
        			System.exit(0);
        		}
        	}
        	parseAndStore(fileList, sExSymbol);
        }
        else
        {
        	logger.error("Error input is not a directory.");
        	System.exit(0);
        }
        logger.info("Hbase data store done!!");
	}
	
	private static String getSymbol(File fInDir)
	{
		return fInDir.getAbsolutePath().substring(fInDir.getAbsolutePath().lastIndexOf("/") + 1);
	}

	private static void usage()
	{
		System.out.println("Usage : app_name  <path/to/config/file>");
	}
	
	private static void parseAndStore(File[] in, String sExSymbol)
	{
		BufferedReader br = null;
		
		for (int i = 0; i < in.length; i++)
		{
			File theFile = in[i];
			if (theFile.exists() && theFile.isFile())
			{
				String symbol = getSymbol(theFile);
				
				// remove extension ".csv" from file name
				symbol = symbol.substring(0, symbol.lastIndexOf("."));
				System.out.println(symbol);
				
				try
				{
					br = new BufferedReader(new FileReader(theFile.getAbsolutePath()));
					String sLine;
					
					// Skippin the header
					br.readLine();
					
					while ((sLine = br.readLine()) != null)
					{
						String[] toks = sLine.split(",");
						
						if (toks.length == 7)
						{
							System.out.println("ExCh  : " + sExSymbol.toUpperCase());
							System.out.println("Sym   : " + symbol.toUpperCase());
							System.out.println("Date  : " + toks[0]);
							System.out.println("Open  : " + toks[1]);
							System.out.println("High  : " + toks[2]);
							System.out.println("Low   : " + toks[3]);
							System.out.println("Close : " + toks[4]);
							System.out.println("Vol   : " + toks[5]);
							System.out.println("AdjVol: " + toks[6]);
							System.out.println("--------------------------------------");
							String sKey = sExSymbol.toUpperCase() + ":" + symbol.toUpperCase() + ":" + toks[0];
							insertHbase("market", sKey, toks[1], toks[2], toks[3], toks[4], toks[5], toks[6]);
						}
						else
						{
							logger.error("Error missing some records.");
							continue;
						}
					}
					br.close();
				}
				catch (IOException e)
				{
					logger.error("Error : reading input file.");
					continue;
				}
			}// end if (theFile.exists() && theFile.isFile())
		}// end for loop
	}
	
	private static void createHbaseTable(String tableName) throws IOException
	{
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		tableDescriptor.addFamily(new HColumnDescriptor("dailyNumbers"));
		admin.createTable(tableDescriptor);
		logger.info("Table Created.");
	}
	
	private static void insertHbase(String tableName, String sKey, String sOpen, String sHigh, String sLow, String sClose, String sVol, String sAdjClose)
	{
		Configuration conf = HBaseConfiguration.create();
		try 
		{
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(sKey));
			put.add(Bytes.toBytes("dailyNumbers"), Bytes.toBytes("open"), Bytes.toBytes(sOpen));
			put.add(Bytes.toBytes("dailyNumbers"), Bytes.toBytes("high"), Bytes.toBytes(sHigh));
			put.add(Bytes.toBytes("dailyNumbers"), Bytes.toBytes("low"), Bytes.toBytes(sLow));
			put.add(Bytes.toBytes("dailyNumbers"), Bytes.toBytes("close"), Bytes.toBytes(sClose));
			put.add(Bytes.toBytes("dailyNumbers"), Bytes.toBytes("vol"), Bytes.toBytes(sVol));
			put.add(Bytes.toBytes("dailyNumbers"), Bytes.toBytes("adjClose"), Bytes.toBytes(sAdjClose));
			table.put(put);
			table.flushCommits();
			table.close();
		} 
		catch (IOException e) 
		{
            logger.error("Error : Inserting data into table.");
			e.printStackTrace();
		}
	}
}
