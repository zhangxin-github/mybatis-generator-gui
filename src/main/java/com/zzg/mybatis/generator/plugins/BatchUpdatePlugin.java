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

import java.util.Iterator;
import java.util.List;
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

public class BatchUpdatePlugin extends PluginAdapter {

	private boolean isOracleDB() {
		String driverClass = context.getJdbcConnectionConfiguration().getDriverClass();
		return "oracle.jdbc.driver.OracleDriver".equals(driverClass);
	}

	private boolean isMysqlDB() {
		String driverClass = context.getJdbcConnectionConfiguration().getDriverClass();
		return "com.mysql.jdbc.Driver".equals(driverClass);
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
		XmlElement answer = this.createBatchUpdateElement(introspectedTable);
		parentElement.addElement(answer);
		answer = this.createBatchUpdateSelectiveElement(introspectedTable);
		parentElement.addElement(answer);
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
		//updateBatchByPrimaryKey方法
		Set importedTypes = new TreeSet();
		Method method = new Method();

		method.setReturnType(new FullyQualifiedJavaType("void"));
		method.setVisibility(JavaVisibility.PUBLIC);
		method.setName("updateBatchByPrimaryKey");

		FullyQualifiedJavaType parameterType = introspectedTable.getRules().calculateAllFieldsClass();

		importedTypes.add(parameterType);

		FullyQualifiedJavaType listParamType = new FullyQualifiedJavaType("java.util.List<" + parameterType + ">");

		method.addParameter(new Parameter(listParamType, "recordLst"));

		this.context.getCommentGenerator().addGeneralMethodComment(method, introspectedTable);

		interfaze.addImportedTypes(importedTypes);
		interfaze.addMethod(method);

		//updateBatchByPrimaryKeySelective方法
		importedTypes = new TreeSet();
		method = new Method();

		method.setReturnType(new FullyQualifiedJavaType("void"));
		method.setVisibility(JavaVisibility.PUBLIC);
		method.setName("updateBatchByPrimaryKeySelective");

		parameterType = introspectedTable.getRules().calculateAllFieldsClass();

		importedTypes.add(parameterType);

		listParamType = new FullyQualifiedJavaType("java.util.List<" + parameterType + ">");

		method.addParameter(new Parameter(listParamType, "recordLst"));

		this.context.getCommentGenerator().addGeneralMethodComment(method, introspectedTable);

		interfaze.addImportedTypes(importedTypes);
		interfaze.addMethod(method);
	}

	/**
	 * 创建批量添加元素updateBatchByPrimaryKey
	 */
	private XmlElement createBatchUpdateElement(IntrospectedTable introspectedTable) {
		XmlElement answer = new XmlElement("update");

		answer.addAttribute(new Attribute("id", "updateBatchByPrimaryKey"));

		FullyQualifiedJavaType parameterType = introspectedTable.getRules().calculateAllFieldsClass();

        answer.addAttribute(new Attribute("parameterType", parameterType.getFullyQualifiedName()));

        context.getCommentGenerator().addComment(answer);

        XmlElement foreachElement = new XmlElement("foreach");
		foreachElement.addAttribute(new Attribute("collection", "list"));
		foreachElement.addAttribute(new Attribute("item", "item"));
		foreachElement.addAttribute(new Attribute("index", "index"));
		if(this.isOracleDB()) {
			//Oracle
			foreachElement.addAttribute(new Attribute("open", "begin"));
			foreachElement.addAttribute(new Attribute("close", ";end;"));
		}
		foreachElement.addAttribute(new Attribute("separator", ";"));

		StringBuilder sb = new StringBuilder();

        sb.append("update "); //$NON-NLS-1$
        sb.append(introspectedTable.getFullyQualifiedTableNameAtRuntime());
        foreachElement.addElement(new TextElement(sb.toString()));

        // set up for first column
        sb.setLength(0);
        sb.append("set "); //$NON-NLS-1$

        Iterator<IntrospectedColumn> iter = introspectedTable
                .getNonPrimaryKeyColumns().iterator();
        while (iter.hasNext()) {
            IntrospectedColumn introspectedColumn = iter.next();

            sb.append(MyBatis3FormattingUtilities
                    .getEscapedColumnName(introspectedColumn));
            sb.append(" = "); //$NON-NLS-1$
            sb.append(MyBatis3FormattingUtilities
                    .getParameterClause(introspectedColumn, "item."));

            if (iter.hasNext()) {
                sb.append(',');
            }

            foreachElement.addElement(new TextElement(sb.toString()));

            // set up for the next column
            if (iter.hasNext()) {
                sb.setLength(0);
                OutputUtilities.xmlIndent(sb, 1);
            }
        }

        boolean and = false;
        for (IntrospectedColumn introspectedColumn : introspectedTable
                .getPrimaryKeyColumns()) {
            sb.setLength(0);
            if (and) {
                sb.append("  and "); //$NON-NLS-1$
            } else {
                sb.append("where "); //$NON-NLS-1$
                and = true;
            }

            /*if(introspectedColumn.isJDBCDateColumn()) {
				if(this.isOracleDB()
						&& "DATE".equalsIgnoreCase(introspectedColumn.getJdbcTypeName())) {
					introspectedColumn.setJdbcTypeName("TIMESTAMP");
				}
			}*/
            sb.append(MyBatis3FormattingUtilities
                    .getEscapedColumnName(introspectedColumn));
            sb.append(" = "); //$NON-NLS-1$
            sb.append(MyBatis3FormattingUtilities
                    .getParameterClause(introspectedColumn, "item."));
            foreachElement.addElement(new TextElement(sb.toString()));
        }
        answer.addElement(foreachElement);
        return answer;
	}

	/**
	 * 创建批量添加元素updateBatchByPrimaryKeySelective
	 */
	private XmlElement createBatchUpdateSelectiveElement(IntrospectedTable introspectedTable) {
		XmlElement answer = new XmlElement("update");

		answer.addAttribute(new Attribute("id", "updateBatchByPrimaryKeySelective"));

		FullyQualifiedJavaType parameterType = introspectedTable.getRules().calculateAllFieldsClass();

        answer.addAttribute(new Attribute("parameterType", parameterType.getFullyQualifiedName()));

        context.getCommentGenerator().addComment(answer);

        XmlElement foreachElement = new XmlElement("foreach");
		foreachElement.addAttribute(new Attribute("collection", "list"));
		foreachElement.addAttribute(new Attribute("item", "item"));
		foreachElement.addAttribute(new Attribute("index", "index"));
		if(this.isOracleDB()) {
			//Oracle
			foreachElement.addAttribute(new Attribute("open", "begin"));
			foreachElement.addAttribute(new Attribute("close", ";end;"));
		}
		foreachElement.addAttribute(new Attribute("separator", ";"));

        StringBuilder sb = new StringBuilder();

        sb.append("update "); //$NON-NLS-1$
        sb.append(introspectedTable.getFullyQualifiedTableNameAtRuntime());
        foreachElement.addElement(new TextElement(sb.toString()));
        //answer.addElement(new TextElement("begin"));

        XmlElement dynamicElement = new XmlElement("set"); //$NON-NLS-1$
        foreachElement.addElement(dynamicElement);

        for (IntrospectedColumn introspectedColumn : introspectedTable
                .getNonPrimaryKeyColumns()) {
        	/*if(introspectedColumn.isJDBCDateColumn()
        			&& "DATE".equalsIgnoreCase(introspectedColumn.getJdbcTypeName())) {
				if(this.isOracleDB()) {
					introspectedColumn.setJdbcTypeName("TIMESTAMP");
				}
			}*/
            XmlElement isNotNullElement = new XmlElement("if"); //$NON-NLS-1$
            sb.setLength(0);
            sb.append("item.").append(introspectedColumn.getJavaProperty());
            sb.append(" != null"); //$NON-NLS-1$
            isNotNullElement.addAttribute(new Attribute("test", sb.toString())); //$NON-NLS-1$
            dynamicElement.addElement(isNotNullElement);

            sb.setLength(0);
            sb.append(MyBatis3FormattingUtilities
                    .getEscapedColumnName(introspectedColumn));
            sb.append(" = "); //$NON-NLS-1$
            sb.append(MyBatis3FormattingUtilities
                    .getParameterClause(introspectedColumn, "item."));
            sb.append(',');

            isNotNullElement.addElement(new TextElement(sb.toString()));
        }

        boolean and = false;
        for (IntrospectedColumn introspectedColumn : introspectedTable
                .getPrimaryKeyColumns()) {
            sb.setLength(0);
            if (and) {
                sb.append("  and "); //$NON-NLS-1$
            } else {
                sb.append("where "); //$NON-NLS-1$
                and = true;
            }

            sb.append(MyBatis3FormattingUtilities
                    .getEscapedColumnName(introspectedColumn));
            sb.append(" = "); //$NON-NLS-1$
            sb.append(MyBatis3FormattingUtilities
                    .getParameterClause(introspectedColumn, "item."));
            foreachElement.addElement(new TextElement(sb.toString()));
        }
        answer.addElement(foreachElement);
        //answer.addElement(new TextElement("end;"));
        return answer;
	}
}
