/**
 * BatchInsertPlugin.java	  V1.0   2016年4月10日 下午9:54:30
 *
 * Copyright (c) 2016 AsiaInfo, All rights reserved.
 *
 * Modification history(By    Time    Reason):
 *
 * Description:
 */

package com.zzg.mybatis.generator.plugins;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.mybatis.generator.api.IntrospectedColumn;
import org.mybatis.generator.api.IntrospectedTable;
import org.mybatis.generator.api.PluginAdapter;
import org.mybatis.generator.api.dom.OutputUtilities;
import org.mybatis.generator.api.dom.java.FullyQualifiedJavaType;
import org.mybatis.generator.api.dom.java.Interface;
import org.mybatis.generator.api.dom.java.JavaVisibility;
import org.mybatis.generator.api.dom.java.Method;
import org.mybatis.generator.api.dom.java.Parameter;
import org.mybatis.generator.api.dom.java.TopLevelClass;
import org.mybatis.generator.api.dom.xml.Attribute;
import org.mybatis.generator.api.dom.xml.Document;
import org.mybatis.generator.api.dom.xml.TextElement;
import org.mybatis.generator.api.dom.xml.XmlElement;
import org.mybatis.generator.codegen.mybatis3.MyBatis3FormattingUtilities;
import org.mybatis.generator.config.GeneratedKey;

public class BatchInsertPlugin extends PluginAdapter {

	private String dbType = "mysql";

	@Override
	public void setProperties(Properties properties) {
		super.setProperties(properties);

		String dbTypeProp = getContext().getProperty("DBType");
		if (dbTypeProp != null)
			this.dbType = dbTypeProp.toLowerCase();
	}

	private boolean isOracleDB() {
		return "oracle".equals(dbType);
	}

	private boolean isMysqlDB() {
		return "mysql".equals(dbType);
	}

	@Override
	public boolean validate(List<String> warnings) {
		return true;
	}

	public boolean clientGenerated(Interface interfaze, TopLevelClass topLevelClass,
			IntrospectedTable introspectedTable) {
		if (introspectedTable.getTargetRuntime() == IntrospectedTable.TargetRuntime.MYBATIS3) {
			addInsertMethod(interfaze, introspectedTable);
		}

		return super.clientGenerated(interfaze, topLevelClass, introspectedTable);
	}

	public boolean sqlMapDocumentGenerated(Document document, IntrospectedTable introspectedTable) {
		if (introspectedTable.getTargetRuntime() == IntrospectedTable.TargetRuntime.MYBATIS3) {
			addElements(document.getRootElement(), introspectedTable);
		}
		return super.sqlMapDocumentGenerated(document, introspectedTable);
	}

	private void addElements(XmlElement parentElement, IntrospectedTable introspectedTable) {
		if(this.isMysqlDB()) {
			//Mysql
			XmlElement answer = this.createBatchInsertElementMysql(introspectedTable);
			parentElement.addElement(answer);
		} else if(this.isOracleDB()) {
			//Oracle
			XmlElement answer = this.createBatchInsertElementOracle(introspectedTable);
			parentElement.addElement(answer);
		}
	}

	protected XmlElement getSelectKey(IntrospectedColumn introspectedColumn, GeneratedKey generatedKey) {
		String identityColumnType = introspectedColumn.getFullyQualifiedJavaType().getFullyQualifiedName();

		XmlElement answer = new XmlElement("selectKey");
		answer.addAttribute(new Attribute("resultType", identityColumnType));
		answer.addAttribute(new Attribute("keyProperty", introspectedColumn.getJavaProperty()));
		answer.addAttribute(new Attribute("order", generatedKey.getMyBatis3Order()));

		answer.addElement(new TextElement(generatedKey.getRuntimeSqlStatement()));

		return answer;
	}

	private void addInsertMethod(Interface interfaze, IntrospectedTable introspectedTable) {
		//insertBatch方法
		Set importedTypes = new TreeSet();
		Method method = new Method();

		method.setReturnType(new FullyQualifiedJavaType("void"));
		method.setVisibility(JavaVisibility.PUBLIC);
		method.setName("insertBatch");

		FullyQualifiedJavaType parameterType = introspectedTable.getRules().calculateAllFieldsClass();

		importedTypes.add(parameterType);

		FullyQualifiedJavaType listParamType = new FullyQualifiedJavaType("java.util.List<" + parameterType + ">");

		method.addParameter(new Parameter(listParamType, "recordLst"));

		this.context.getCommentGenerator().addGeneralMethodComment(method, introspectedTable);

		interfaze.addImportedTypes(importedTypes);
		interfaze.addMethod(method);
	}

	/**
	 * 创建Mysql批量添加元素insertBatch
	 */
	private XmlElement createBatchInsertElementMysql(IntrospectedTable introspectedTable) {
		XmlElement answer = new XmlElement("insert");

		answer.addAttribute(new Attribute("id", "insertBatch"));

		FullyQualifiedJavaType parameterType = introspectedTable.getRules().calculateAllFieldsClass();

		answer.addAttribute(new Attribute("parameterType", parameterType.getFullyQualifiedName()));

		this.context.getCommentGenerator().addComment(answer);

		GeneratedKey gk = introspectedTable.getGeneratedKey();
		if (gk != null) {
			IntrospectedColumn introspectedColumn = introspectedTable.getColumn(gk.getColumn());

			if (introspectedColumn != null) {
				if (gk.isJdbcStandard()) {
					answer.addAttribute(new Attribute("useGeneratedKeys", "true"));
					answer.addAttribute(new Attribute("keyProperty", introspectedColumn.getJavaProperty()));
				} else {
					answer.addElement(getSelectKey(introspectedColumn, gk));
				}
			}
		}

		StringBuilder insertClause = new StringBuilder();
		StringBuilder valuesClause = new StringBuilder();

		insertClause.append("insert into ");
		insertClause.append(introspectedTable.getFullyQualifiedTableNameAtRuntime());
		insertClause.append(" (");

		valuesClause.append("(");

		XmlElement foreachElement = new XmlElement("foreach");
		foreachElement.addAttribute(new Attribute("collection", "list"));
		foreachElement.addAttribute(new Attribute("item", "item"));
		foreachElement.addAttribute(new Attribute("index", "index"));
		foreachElement.addAttribute(new Attribute("separator", ","));

		List<String> valuesClauses = new ArrayList<String>();
		Iterator iter = introspectedTable.getAllColumns().iterator();
		while (iter.hasNext()) {
			IntrospectedColumn introspectedColumn = (IntrospectedColumn) iter.next();
			if (introspectedColumn.isIdentity()) {
				continue;
			}

			insertClause.append(MyBatis3FormattingUtilities.getEscapedColumnName(introspectedColumn));
			valuesClause.append(MyBatis3FormattingUtilities.getParameterClause(introspectedColumn, "item."));
			if (iter.hasNext()) {
				insertClause.append(", ");
				valuesClause.append(", ");
			}

			if (valuesClause.length() > 80) {
				answer.addElement(new TextElement(insertClause.toString()));
				insertClause.setLength(0);
				OutputUtilities.xmlIndent(insertClause, 1);

				valuesClauses.add(valuesClause.toString());
				valuesClause.setLength(0);
				OutputUtilities.xmlIndent(valuesClause, 1);
			}
		}

		insertClause.append(") values ");
		answer.addElement(new TextElement(insertClause.toString()));

		valuesClause.append(")");
		valuesClauses.add(valuesClause.toString());

		for (String clause : valuesClauses) {
			foreachElement.addElement(new TextElement(clause));
		}
		answer.addElement(foreachElement);

		return answer;
	}

	/**
	 * 创建Oracle批量添加元素insertBatch
	 */
	private XmlElement createBatchInsertElementOracle(IntrospectedTable introspectedTable) {
		XmlElement answer = new XmlElement("insert");

		answer.addAttribute(new Attribute("id", "insertBatch"));

		FullyQualifiedJavaType parameterType = introspectedTable.getRules().calculateAllFieldsClass();

		answer.addAttribute(new Attribute("parameterType", parameterType.getFullyQualifiedName()));

		this.context.getCommentGenerator().addComment(answer);

		GeneratedKey gk = introspectedTable.getGeneratedKey();
		if (gk != null) {
			IntrospectedColumn introspectedColumn = introspectedTable.getColumn(gk.getColumn());

			if (introspectedColumn != null) {
				if (gk.isJdbcStandard()) {
					answer.addAttribute(new Attribute("useGeneratedKeys", "true"));
					answer.addAttribute(new Attribute("keyProperty", introspectedColumn.getJavaProperty()));
				} else {
					answer.addElement(getSelectKey(introspectedColumn, gk));
				}
			}
		}

		StringBuilder insertClause = new StringBuilder();
		StringBuilder valuesClause = new StringBuilder();

		insertClause.append("insert into ");
		insertClause.append(introspectedTable.getFullyQualifiedTableNameAtRuntime());
		insertClause.append(" (");

		valuesClause.append("select ");

		XmlElement foreachElement = new XmlElement("foreach");
		foreachElement.addAttribute(new Attribute("collection", "list"));
		foreachElement.addAttribute(new Attribute("item", "item"));
		foreachElement.addAttribute(new Attribute("index", "index"));
		foreachElement.addAttribute(new Attribute("open", "("));
		foreachElement.addAttribute(new Attribute("close", ")"));
		foreachElement.addAttribute(new Attribute("separator", "union all"));

		List<String> valuesClauses = new ArrayList<String>();
		Iterator iter = introspectedTable.getAllColumns().iterator();
		while (iter.hasNext()) {
			IntrospectedColumn introspectedColumn = (IntrospectedColumn) iter.next();
			if (introspectedColumn.isIdentity()) {
				continue;
			}
			/*if(introspectedColumn.isJDBCDateColumn()
					&& "DATE".equalsIgnoreCase(introspectedColumn.getJdbcTypeName())) {
				if(this.isOracleDB()) {
					introspectedColumn.setJdbcTypeName("TIMESTAMP");
				}
			}*/

			insertClause.append(MyBatis3FormattingUtilities.getEscapedColumnName(introspectedColumn));
			valuesClause.append(MyBatis3FormattingUtilities.getParameterClause(introspectedColumn, "item."));
			if (iter.hasNext()) {
				insertClause.append(", ");
				valuesClause.append(", ");
			}

			if (valuesClause.length() > 80) {
				answer.addElement(new TextElement(insertClause.toString()));
				insertClause.setLength(0);
				OutputUtilities.xmlIndent(insertClause, 1);

				valuesClauses.add(valuesClause.toString());
				valuesClause.setLength(0);
				OutputUtilities.xmlIndent(valuesClause, 1);
			}
		}

		insertClause.append(") ");
		answer.addElement(new TextElement(insertClause.toString()));

		valuesClause.append(" from dual");
		valuesClauses.add(valuesClause.toString());

		for (String clause : valuesClauses) {
			foreachElement.addElement(new TextElement(clause));
		}
		answer.addElement(foreachElement);

		return answer;
	}
}
