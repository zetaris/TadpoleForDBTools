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
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
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
import com.mongodb.BasicDBObject;
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
				
				final String strTargetDir = /* "/Users/hangum/Downloads/mon/"*/ System.getProperty("user.home") + "/mon/" + getSourceUserDB().getDisplay_name() + "/";
				
				// 기존에 데이터가 있다면 삭제합니다.
				try {
					FileUtils.deleteDirectory(new File(strTargetDir));
				} catch (IOException e1) {
					logger.debug("delete directory");
				}
				
				// 없다면 새롭게 만듭니다. 
				new File(strTargetDir).mkdirs();
				
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
						insertMongoDB(strTargetDir, modTableDAO, strNewColName);
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
			
	private void insertMongoDB(String strTargetDir, ModTableDAO modTableDAO, String strNewColName) throws Exception {
		
		String workTable = modTableDAO.getName();		
		if(logger.isDebugEnabled()) logger.debug("[work collection]" + workTable);
		
		logger.debug(getSourceUserDB().getDBDefine() + ", target is " + getTargetUserDB().getDBDefine());
		if(getSourceUserDB().getDBDefine() == DBDefine.MONGODB_DEFAULT && getTargetUserDB().getDBDefine() != DBDefine.MONGODB_DEFAULT) {
			if(logger.isDebugEnabled()) logger.debug("=========> source db is mongo, target db is does not mongo");
			long longStart = System.currentTimeMillis();
			// create 문을 만들기위해  컬럼 구조를 넣습니다. 
			Map<String, Map<String, CollectionFieldDAO>> mapTableColumn = new HashMap<String, Map<String, CollectionFieldDAO>>();
			
			if(isCreateTable) {
				// 전체 스크립트를 뽑아야 한다.
				// create 문을 뽑기 위해 필드 명을 찾아서 한 행으로 만들어 준다.
				MongoDBQueryUtil qu = new MongoDBQueryUtil(getSourceUserDB(), workTable);
				if(logger.isDebugEnabled()) logger.debug("---start object-----");
				while(qu.hasNext()) {
					qu.nextQuery();
					
					// row 단위
					List<DBObject> listDBObject = qu.getCollectionDataList();
					if(logger.isDebugEnabled()) logger.debug("\t[start]generate create statement : " + listDBObject.size());
					for (DBObject dbObject : listDBObject) {
						Map<String, List<CollectionFieldDAO>> mapCollectionInfo = MongoDBTableColumn.tableColumnInfoFlat(strNewColName, dbObject);
						for (String strKey : mapCollectionInfo.keySet()) {
							Map<String, CollectionFieldDAO> tmpTableColumn = mapTableColumn.get(strKey);
							// collection 이 없으면...
							if(tmpTableColumn == null) {
								tmpTableColumn = new HashMap<String,CollectionFieldDAO>();
								mapTableColumn.put(strKey, tmpTableColumn);
							}
							
							for (CollectionFieldDAO collectionFieldDAO : mapCollectionInfo.get(strKey)) {
								if(!tmpTableColumn.containsKey(collectionFieldDAO.getField())) {
									tmpTableColumn.put(collectionFieldDAO.getField(), collectionFieldDAO);
								}
							}	
						}
						
					}
					if(logger.isDebugEnabled()) logger.debug("\t[end]generate create statement : " );
				}
				
				for (String strTableName : mapTableColumn.keySet()) {
					String strErrorTxt = "";
					// 실제 모든 컬럼 정보이다.
					String strCreateStatementColumn = "";
					
					Map<String, CollectionFieldDAO> tmpTableColumn = mapTableColumn.get(strTableName);
					for (String strColumn : tmpTableColumn.keySet()) {
						
						CollectionFieldDAO collectField = tmpTableColumn.get(strColumn);
		//				System.out.println("====>" + collectField.getField() + "\t" + collectField.getType());
						
						String strCollectFieldType = collectField.getType();
						String strCollectField = collectField.getField();
						
						if("ObjectID".equalsIgnoreCase(strCollectFieldType) || "String".equalsIgnoreCase(strCollectFieldType)) {
							strCreateStatementColumn += strCollectField + " varchar(400), "; 
						} else if("Date".equalsIgnoreCase(strCollectFieldType)) {
							strCreateStatementColumn += strCollectField + " Timestamp, ";
						} else if("Integer".equalsIgnoreCase(strCollectFieldType) || "Double".equalsIgnoreCase(strCollectFieldType)) {
							strCreateStatementColumn += strCollectField + " BIGINT, ";
						} else if("Boolean".equalsIgnoreCase(strCollectFieldType)) {
							strCreateStatementColumn += strCollectField + " int(1), ";
						} else if("BasicBSONList".equalsIgnoreCase(strCollectFieldType)) {
							strCreateStatementColumn += strCollectField + " varchar(400), ";
	//					} else { //BasicDBObject
	//						strErrorTxt += "igonr column is " + strCollectField + ", column type is " + strCollectFieldType + PublicTadpoleDefine.LINE_SEPARATOR;;
						}
					}
					
					logger.debug("==========> table create ended..." + strTargetDir);
					strCreateStatementColumn = StringUtils.removeEnd(strCreateStatementColumn, ", ");
					
					String createStatement = String.format(CREATE_STATEMNT, strTableName, strCreateStatementColumn);
					logger.debug(createStatement);
					new File(strTargetDir + strTableName + ".sql").createNewFile();
					FileUtils.writeStringToFile(new File(strTargetDir + strTableName + ".sql"), createStatement, true);
					FileUtils.writeStringToFile(new File(strTargetDir + strTableName + ".sql"), "/*" + strErrorTxt + "*/" + PublicTadpoleDefine.LINE_SEPARATOR, true);
				}
			}
			
			if(isInsertStatment) {
				//insert 문을 생성합니다.
				MongoDBQueryUtil qu2 = new MongoDBQueryUtil(getSourceUserDB(), workTable);
				StringBuffer sbBufferColumn = new StringBuffer();
				StringBuffer sbBufferValue = new StringBuffer();

				while(qu2.hasNext()) {
					qu2.nextQuery();
					
					// row 단위
					List<DBObject> listDBObject = qu2.getCollectionDataList();
					for (DBObject dbObject : listDBObject) {
						Map<String, List<CollectionFieldDAO>> mapCollectionInfo = MongoDBTableColumn.tableColumnInfoFlat(strNewColName, dbObject);
						
						for (String strKey : mapCollectionInfo.keySet()) {
							logger.debug("=[start]\t" + strKey);
							boolean isBasicBSONList = false;
						
							sbBufferColumn.setLength(0);
							sbBufferValue.setLength(0);
		
							List<CollectionFieldDAO> listCollectionInfo = mapCollectionInfo.get(strKey);
							for (CollectionFieldDAO collectionFieldDAO : listCollectionInfo) {
								
								String strCollectFieldType = collectionFieldDAO.getType();
								Object strValue = dbObject.get(collectionFieldDAO.getSearchName());
								logger.debug("column name is [" + collectionFieldDAO.getField() + "] type is [" + collectionFieldDAO.getType() + "] values is [" + strValue + "]");
								
								if("ObjectID".equalsIgnoreCase(strCollectFieldType) || "String".equalsIgnoreCase(strCollectFieldType)) {
									sbBufferColumn.append(collectionFieldDAO.getField()).append(", ");
									
									if(strValue != null) {
										String strTmpValue = strValue == null?"":strValue.toString();

										//	http://www.mysqlkorea.com/sub.html?mcode=manual&scode=01&m_no=21571&cat1=9&cat2=290&cat3=295&lang=k
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
								} else if("BasicBSONList".equals(strCollectFieldType)) {
									logger.debug("========> basicBsonlist \t" + collectionFieldDAO.getSearchName() + ":" + collectionFieldDAO.getField());
									
									String strRealKey = StringUtils.remove(strKey, strNewColName+"_");

									/*
									 * list 형태의 데이터의 list데이터 하나가 하나의 insert문이 되도록 작업 되었습니다.
									 * 하여서 데이터를 돌면서 insert into문을 생성해 주면됩니다.
									 */
									isBasicBSONList = true;
									BasicBSONList basicList = (BasicBSONList)dbObject.get(strRealKey);
									if(!basicList.isEmpty()) {
										sbBufferColumn.setLength(0);
										sbBufferValue.setLength(0);
										
										// insert into (a, b, c) 문의 컬럼명을 넣는 부분을 만듭니다. 
										for (CollectionFieldDAO innerCollectionField : listCollectionInfo) {
											sbBufferColumn.append(innerCollectionField.getField()).append(", ");
										}
										String strCName = StringUtils.removeEnd(sbBufferColumn.toString(), ", ");
										
										
										for (Object objArryValue : basicList) {
											sbBufferValue.setLength(0);
											
											if(objArryValue instanceof String | objArryValue instanceof ObjectId) {
												sbBufferValue.append(objArryValue == null?"''":"'" + objArryValue.toString() + "', ");
												// _id 키값을 입력합니다.
												sbBufferValue.append("'" + dbObject.get("_id").toString() + "', ");
														
											} else {
												BasicDBObject basicObject = (BasicDBObject)objArryValue;
												
												for (CollectionFieldDAO innerCollectionField : listCollectionInfo) {
													if("_id".equals(innerCollectionField.getSearchName())) {
														sbBufferValue.append("'" + dbObject.get("_id").toString() + "', ");		
													} else {
														String strInnerValue = basicObject.get(innerCollectionField.getSearchName()) == null?"":basicObject.get(innerCollectionField.getSearchName()).toString();
														sbBufferValue.append(strInnerValue == null?"''":"'" + strInnerValue + "', ");	
													}
												}
											}
											
											String strCValue = StringUtils.removeEnd(sbBufferValue.toString(), ", ");
											String strInsertStatement = String.format(INSERT_INTO, strKey, strCName, strCValue);
											FileUtils.writeStringToFile(new File(strTargetDir + strKey + ".sql"), strInsertStatement, true);
										}
										
									}
									
									break;
								} else {// if("BasicDBObject".equalsIgnoreCase(strCollectFieldType)) { 
		//							strErrorTxt += "igonr column is " + strCollectField + ", column type is " + strCollectFieldType + PublicTadpoleDefine.LINE_SEPARATOR;;
								}
							}
							
							if(!isBasicBSONList) {
								String strCName = StringUtils.removeEnd(sbBufferColumn.toString(), ", ");
								String strCValue = StringUtils.removeEnd(sbBufferValue.toString(), ", ");
								String strInsertStatement = String.format(INSERT_INTO, strKey, strCName, strCValue);
								FileUtils.writeStringToFile(new File(strTargetDir + strKey + ".sql"), strInsertStatement, true);
							}
						}	// end first statement
					}	// end last statement
					
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