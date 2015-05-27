/*******************************************************************************
 * Copyright (c) 2013 hangum.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser Public License v2.1
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 * 
 * Contributors:
 *     hangum - initial API and implementation
 ******************************************************************************/
package com.hangum.tadpole.importdb.core.dialog.importdb.mongodb;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.jface.dialogs.MessageDialog;

import com.hangum.tadpold.commons.libs.core.define.PublicTadpoleDefine;
import com.hangum.tadpole.commons.util.Utils;
import com.hangum.tadpole.engine.define.DBDefine;
import com.hangum.tadpole.engine.query.dao.mongodb.CollectionFieldDAO;
import com.hangum.tadpole.engine.query.dao.system.UserDBDAO;
import com.hangum.tadpole.engine.sql.util.QueryUtils;
import com.hangum.tadpole.importdb.Activator;
import com.hangum.tadpole.importdb.core.dialog.importdb.dao.ModTableDAO;
import com.hangum.tadpole.importdb.core.dialog.importdb.utils.MongoDBQueryUtil;
import com.hangum.tadpole.mongodb.core.query.MongoDBQuery;
import com.hangum.tadpole.mongodb.core.utils.MongoDBTableColumn;
import com.mongodb.DBObject;

/**
 * mongodb collection을 mongodb로 넘깁니다.
 * 
 * @author hangum
 *
 */
public class MongoDBCollectionToMongodBImport extends DBImport {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(MongoDBCollectionToMongodBImport.class);
	
	private List<ModTableDAO> listModeTable; 
	private boolean isTableCreateion = false;
	private boolean isImportStatemennt = false;
	

	/**
	 * @return the isTableCreateion
	 */
	public boolean isTableCreateion() {
		return isTableCreateion;
	}

	/**
	 * @param isTableCreateion the isTableCreateion to set
	 */
	public void setTableCreateion(boolean isTableCreateion) {
		this.isTableCreateion = isTableCreateion;
	}

	/**
	 * @return the isImportStatemennt
	 */
	public boolean isImportStatemennt() {
		return isImportStatemennt;
	}

	/**
	 * @param isImportStatemennt the isImportStatemennt to set
	 */
	public void setImportStatemennt(boolean isImportStatemennt) {
		this.isImportStatemennt = isImportStatemennt;
	}

	/**
	 * 
	 * @param sourceUserDB
	 * @param targetUserDB
	 * @param listModeTable
	 */
	public MongoDBCollectionToMongodBImport(UserDBDAO sourceUserDB, UserDBDAO targetUserDB, List<ModTableDAO> listModeTable) {
		super(sourceUserDB, targetUserDB);
		
		this.listModeTable = listModeTable;
	}


	/**
	 * @param sourceDBDAO
	 * @param targetDBDAO
	 * @param selectListTables
	 * @param selection
	 * @param selection2
	 */
	boolean isCreateTable = true;
	boolean isInsertStatment = true;
	public MongoDBCollectionToMongodBImport(UserDBDAO sourceDBDAO, UserDBDAO targetDBDAO,
			List<ModTableDAO> listModeTable, boolean isCreateTable, boolean isInsertStatment) {
		super(sourceDBDAO, targetDBDAO);
		
		this.listModeTable = listModeTable;
		this.isCreateTable = isCreateTable;
		this.isInsertStatment = isInsertStatment;
	}

	@Override
	/**
	 * table import
	 */
	public Job workTableImport() {
		if(0 == listModeTable.size()) {
			MessageDialog.openInformation(null, "Confirm", "Please select table");
			return null;
		}
		
		// job
		Job job = new Job("Execute data Import.") {
			@Override
			public IStatus run(IProgressMonitor monitor) {
				monitor.beginTask("Start import....", IProgressMonitor.UNKNOWN);
				
				try {
					for (ModTableDAO modTableDAO : listModeTable) {

						monitor.subTask(modTableDAO.getName() + " importing...");
						
						// collection is exist on delete.
						String strNewColName = modTableDAO.getReName().trim().equals("")?modTableDAO.getName():modTableDAO.getReName();
						if(modTableDAO.isExistOnDelete()) {
							if(getSourceUserDB().getDBDefine() == DBDefine.MONGODB_DEFAULT) MongoDBQuery.existOnDelete(getSourceUserDB(), modTableDAO.getName());
							else QueryUtils.executeDroptable(getTargetUserDB(), "drop table " + modTableDAO.getName());
						}
						
						// insert
						insertMongoDB(modTableDAO, strNewColName);
					}			

				} catch(Exception e) {
					logger.error("press ok button", e);						
					return new Status(IStatus.ERROR, Activator.PLUGIN_ID, e.getMessage(), e); //$NON-NLS-1$
				} finally {
					monitor.done();
				}
				
				return Status.OK_STATUS;
			}
		};
		
		return job;
	}
	
	/**
	 * 데이터를 입력합니다.
	 * 
	 * @param modTableDAO
	 * @param userDBDAO
	 * @throws Exception
	 */
	public static final String CREATE_STATEMNT = "CREATE TABLE %s(%s); " + PublicTadpoleDefine.LINE_SEPARATOR;
	public static final String INSERT_INTO = "INSERT INTO %s (%s) VALUES (%s);" + PublicTadpoleDefine.LINE_SEPARATOR;
			
	private void insertMongoDB(ModTableDAO modTableDAO, String strNewColName) throws Exception {
		String strUserHome = System.getProperty("user.home");
		String strDir = /* "/Users/hangum/Downloads/mon/"*/ strUserHome + "/mon/" + getSourceUserDB().getDisplay_name() + "/";
		new File(strDir).mkdirs();
		String strFileName =  strDir + strNewColName + ".sql";
		logger.debug("file location is " + strFileName);
		new File(strFileName).delete();
		
		String workTable = modTableDAO.getName();		
		if(logger.isDebugEnabled()) logger.debug("[work collection]" + workTable);
		
		logger.debug(getSourceUserDB().getDBDefine() + ", target is " + getTargetUserDB().getDBDefine());
		if(getSourceUserDB().getDBDefine() == DBDefine.MONGODB_DEFAULT && getTargetUserDB().getDBDefine() != DBDefine.MONGODB_DEFAULT) {
			logger.debug("=========> source db is mongo, target db is does not mongo");
			long longStart = System.currentTimeMillis();
			// create 문을 만들기위해  컬럼 구조를 넣습니다. 
			Map<String, CollectionFieldDAO> mapColumn = new HashMap<String, CollectionFieldDAO>();
			
			if(isCreateTable) {
				// 전체 스크립트를 뽑아야 한다.
				// create 문을 뽑기 위해 필드 명을 찾아서 한 행으로 만들어 준다.
				MongoDBQueryUtil qu = new MongoDBQueryUtil(getSourceUserDB(), workTable);
				logger.debug("---start object-----");
				while(qu.hasNext()) {
					qu.nextQuery();
					
					// row 단위
					List<DBObject> listDBObject = qu.getCollectionDataList();
					logger.debug("\t[start]generate create statement : " + listDBObject.size());
					for (DBObject dbObject : listDBObject) {
//						long subS = System.currentTimeMillis();
						List<CollectionFieldDAO> listCollectionInfo = MongoDBTableColumn.tableColumnInfoFlat(dbObject);
//						logger.debug("=>1. " + (System.currentTimeMillis() - subS));
//						
//						subS = System.currentTimeMillis();
//						logger.debug("\t\t field count : " + listCollectionInfo.size());
						for (CollectionFieldDAO collectionFieldDAO : listCollectionInfo) {
							if(!mapColumn.containsKey(collectionFieldDAO.getField())) {
								mapColumn.put(collectionFieldDAO.getField(), collectionFieldDAO);
							}
						}
//						logger.debug("=>2. " + (System.currentTimeMillis() - subS));
					}
					logger.debug("\t[end]generate create statement : " );
	//				MongoDBQuery.insertDocument(getTargetUserDB(), strNewColName, listDBObject);
				}
				
				String strErrorTxt = "";
				// 실제 모든 컬럼 정보이다.
				String strColumn = "";
				Set<String> keySet = mapColumn.keySet();
				for (String string : keySet) {
					CollectionFieldDAO collectField = mapColumn.get(string);
	//				System.out.println("====>" + collectField.getField() + "\t" + collectField.getType());
					
					String strCollectFieldType = collectField.getType();
					String strCollectField = collectField.getField();
					
					if("ObjectID".equalsIgnoreCase(strCollectFieldType) || "String".equalsIgnoreCase(strCollectFieldType)) {
						strColumn += strCollectField + " varchar(2000), "; 
					} else if("Date".equalsIgnoreCase(strCollectFieldType)) {
						strColumn += strCollectField + " Timestamp, ";
					} else if("Integer".equalsIgnoreCase(strCollectFieldType) || "Double".equalsIgnoreCase(strCollectFieldType)) {
						strColumn += strCollectField + " BIGINT, ";
					} else if("Boolean".equalsIgnoreCase(strCollectFieldType)) {
						strColumn += strCollectField + " int(1), ";
					} else { //BasicDBObject
						strErrorTxt += "igonr column is " + strCollectField + ", column type is " + strCollectFieldType + PublicTadpoleDefine.LINE_SEPARATOR;;
					}
					
				}
				logger.debug("==========> table create ended...");
				strColumn = StringUtils.removeEnd(strColumn, ", ");
				
				String createStatement = String.format(CREATE_STATEMNT, strNewColName, strColumn);
				logger.debug(createStatement);
				new File(strFileName).createNewFile();
				FileUtils.writeStringToFile(new File(strFileName), createStatement, true);
				FileUtils.writeStringToFile(new File(strFileName), "/*" + strErrorTxt + "*/" + PublicTadpoleDefine.LINE_SEPARATOR, true);
			}
			
			if(isInsertStatment) {
				//insert 문을 생성합니다.
				MongoDBQueryUtil qu2 = new MongoDBQueryUtil(getSourceUserDB(), workTable);
				int i =0;
				while(qu2.hasNext()) {
					qu2.nextQuery();
					
					// row 단위
					List<DBObject> listDBObject = qu2.getCollectionDataList();
					for (DBObject dbObject : listDBObject) {
						List<CollectionFieldDAO> listCollectionInfo = MongoDBTableColumn.tableColumnInfoFlat(dbObject);
						StringBuffer sbBufferColumn = new StringBuffer();
						StringBuffer sbBufferValue = new StringBuffer();
	
//						logger.debug("\tinsert count is " + i++);
						
						
						for (CollectionFieldDAO collectionFieldDAO : listCollectionInfo) {
							
							String strCollectFieldType = collectionFieldDAO.getType();
							Object strValue = dbObject.get(collectionFieldDAO.getSearchName());
	//						logger.debug("column name is [" + collectionFieldDAO.getField() + "] value is [" + collectionFieldDAO.getType() + "] values is [" + strValue + "]");
							
							if("ObjectID".equalsIgnoreCase(strCollectFieldType) || "String".equalsIgnoreCase(strCollectFieldType)) {
								sbBufferColumn.append(collectionFieldDAO.getField()).append(", ");
								
								if(strValue != null) {
									String strTmpValue = strValue.toString();
	//								http://www.mysqlkorea.com/sub.html?mcode=manual&scode=01&m_no=21571&cat1=9&cat2=290&cat3=295&lang=k
									
									strTmpValue = StringEscapeUtils.escapeSql(strTmpValue);
									strTmpValue = StringHelper.escapeSQL(strTmpValue);
									
									sbBufferValue.append("'" + strTmpValue + "'").append(", ");
								} else {
									sbBufferValue.append("''").append(", ");
								}
								
							} else if("Date".equalsIgnoreCase(strCollectFieldType)) {
								sbBufferColumn.append(collectionFieldDAO.getField()).append(", ");
	
								if(strValue != null) {
									java.util.Date date = (java.util.Date)strValue;
									String strDate = Utils.dateToStr(date);
									sbBufferValue.append("STR_TO_DATE('" + strDate + "', '%Y-%m-%d %H:%i:%s')").append(", ");
								} else {
									sbBufferValue.append("").append(", ");
								}
								
								
							} else if("Integer".equalsIgnoreCase(strCollectFieldType) || "Double".equalsIgnoreCase(strCollectFieldType)) {
								
								sbBufferColumn.append(collectionFieldDAO.getField()).append(", ");
								
								if(strValue != null) {
									sbBufferValue.append(dbObject.get(collectionFieldDAO.getField())).append(", ");
								} else {
									sbBufferValue.append(0).append(", ");
								}
								
							} else if("Boolean".equalsIgnoreCase(strCollectFieldType)) {
								sbBufferColumn.append(collectionFieldDAO.getField()).append(", ");
								
								if(strValue != null) {
									if(Boolean.parseBoolean(strValue.toString())) {
										sbBufferValue.append("1").append(", ");
									} else {
										sbBufferValue.append("0").append(", ");
									}
								} else {
									sbBufferValue.append("0").append(", ");
								}
								
								
							} else {// if("BasicDBObject".equalsIgnoreCase(strCollectFieldType)) { 
	//							strErrorTxt += "igonr column is " + strCollectField + ", column type is " + strCollectFieldType + PublicTadpoleDefine.LINE_SEPARATOR;;
							}
							
							
						}
						
						String strCName = StringUtils.removeEnd(sbBufferColumn.toString(), ", ");
						String strCValue = StringUtils.removeEnd(sbBufferValue.toString(), ", ");
						String strInsertStatement = String.format(INSERT_INTO, strNewColName, strCName, strCValue);
	//					logger.debug(strInsertStatement);
						FileUtils.writeStringToFile(new File(strFileName), strInsertStatement, true);
					}
					
					long longEnd = System.currentTimeMillis();
					logger.info(strNewColName + "=> " + ((longEnd - longStart)/1000) + " second");
				}
			}
			
		} else {
			
			MongoDBQueryUtil qu = new MongoDBQueryUtil(getSourceUserDB(), workTable);
			while(qu.hasNext()) {
				qu.nextQuery();
				
				// row 단위
				List<DBObject> listDBObject = qu.getCollectionDataList();
				if(logger.isDebugEnabled()) logger.debug("[work table]" + strNewColName + " size is " + listDBObject.size());
			
				MongoDBQuery.insertDocument(getTargetUserDB(), strNewColName, listDBObject);
			}
		}
	}
	
}