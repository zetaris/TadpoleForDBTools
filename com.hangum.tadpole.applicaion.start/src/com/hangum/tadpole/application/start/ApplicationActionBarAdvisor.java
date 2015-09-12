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
package com.hangum.tadpole.application.start;

import org.eclipse.jface.action.GroupMarker;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.action.ICoolBarManager;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.IToolBarManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.action.ToolBarContributionItem;
import org.eclipse.jface.action.ToolBarManager;
import org.eclipse.swt.SWT;
import org.eclipse.ui.IWorkbenchActionConstants;
import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.actions.ActionFactory;
import org.eclipse.ui.actions.ActionFactory.IWorkbenchAction;
import org.eclipse.ui.application.ActionBarAdvisor;
import org.eclipse.ui.application.IActionBarConfigurer;

import com.hangum.tadpole.application.start.action.AboutAction;
import com.hangum.tadpole.application.start.action.BugIssueAction;
import com.hangum.tadpole.commons.admin.core.actions.SendMessageAction;
import com.hangum.tadpole.commons.admin.core.actions.UserLoginHistoryAction;
import com.hangum.tadpole.engine.permission.PermissionChecker;
import com.hangum.tadpole.manager.core.actions.global.ExecutedSQLAction;
import com.hangum.tadpole.manager.core.actions.global.ResourceManagerAction;
import com.hangum.tadpole.manager.core.actions.global.RestfulAPIManagerAction;
import com.hangum.tadpole.manager.core.actions.global.SchemaHistoryAction;
import com.hangum.tadpole.manager.core.actions.global.TransactionConnectionManagerAction;
import com.hangum.tadpole.manager.core.actions.global.UserPermissionAction;
import com.hangum.tadpole.rdb.core.actions.global.ConnectDatabaseAction;
import com.hangum.tadpole.rdb.core.actions.global.DeleteResourceAction;
import com.hangum.tadpole.rdb.core.actions.global.ExitAction;
import com.hangum.tadpole.rdb.core.actions.global.OpenDBRelationAction;
import com.hangum.tadpole.rdb.core.actions.global.OpenQueryEditorAction;
import com.hangum.tadpole.rdb.core.actions.global.PreferenceAction;
import com.hangum.tadpole.session.manager.SessionManager;

/**
 * Define at action, toolbar, menu
 * 
 * @author hangum
 */
public class ApplicationActionBarAdvisor extends ActionBarAdvisor {
    private IWorkbenchAction exitAction;
    
    private IAction saveAction;
    private IAction saveAsAction;
    
    private IAction connectAction;
    private IAction queryOpenAction;
    private IAction dbRelationOpenAction;
    private IAction deleteResourceAction;
    
    private IAction restFulAPIAction;
    
    /** send message */
    private IAction sendMessageAction;
    private IAction userLoginHistoryAction;
    
    /** User permission action */
    private IAction userPermissionAction;
    
    /** transaction connection */
    private IAction transactionConnectionAction;
    
    /** executed sql */
    private IAction executedSQLAction;
    
    /** schedule action */
//    private IAction monitoringRealTimeAction;
    
    /** schema history */
    private IAction schemaHistoryAction;
    
    private IAction resourceManageAction;
    
    private IAction preferenceAction;
    private IAction aboutAction;
    private IAction bugIssueAction;
    

    public ApplicationActionBarAdvisor(IActionBarConfigurer configurer) {
        super(configurer);
    }
    
    protected void makeActions(final IWorkbenchWindow window) {
   	
    	saveAction = ActionFactory.SAVE.create(window);
    	register(saveAction);
    	
    	saveAsAction = ActionFactory.SAVE_AS.create(window);
    	register(saveAsAction);
    	
    	connectAction = new ConnectDatabaseAction(window);
    	register(connectAction);
    	
    	queryOpenAction = new OpenQueryEditorAction(window);
    	register(queryOpenAction);
    	
    	dbRelationOpenAction = new OpenDBRelationAction(window);
    	register(dbRelationOpenAction);
    	
    	deleteResourceAction = new DeleteResourceAction(window);
    	register(deleteResourceAction);
    	
    	sendMessageAction = new SendMessageAction(window);
    	register(sendMessageAction);
    	
    	userLoginHistoryAction = new UserLoginHistoryAction(window);
    	register(userLoginHistoryAction);
    	
    	userPermissionAction = new UserPermissionAction(window);
    	register(userPermissionAction);
    	
    	transactionConnectionAction = new TransactionConnectionManagerAction(window);
    	register(transactionConnectionAction);
    	
    	executedSQLAction = new ExecutedSQLAction(window);
    	register(executedSQLAction);
    	    	
    	schemaHistoryAction = new SchemaHistoryAction(window);
    	register(schemaHistoryAction);
    	
    	resourceManageAction = new ResourceManagerAction(window);
    	register(resourceManageAction);
    	
    	restFulAPIAction = new RestfulAPIManagerAction(window);
    	register(restFulAPIAction);

        exitAction = new ExitAction(window);
        register(exitAction);
        
        preferenceAction = new PreferenceAction(window);
        register(preferenceAction);        
        
        aboutAction = new AboutAction(window);
        register(aboutAction);
        
        bugIssueAction = new BugIssueAction(window);
        register(bugIssueAction);
        
    }
    
    /**
     * Comment at 2.1 RC3 has error(https://bugs.eclipse.org/bugs/show_bug.cgi?id=410260) 
     */
    protected void fillMenuBar(IMenuManager menuBar) {
    	MenuManager fileMenu = new MenuManager("&File", IWorkbenchActionConstants.M_FILE);
    	MenuManager manageMenu = new MenuManager("&Manage", IWorkbenchActionConstants.M_PROJECT);
    	MenuManager adminMenu = null;
    	
    	boolean isAdmin = PermissionChecker.isAdmin(SessionManager.getRepresentRole());
    	if(isAdmin) {
    		adminMenu = new MenuManager("&Admin", IWorkbenchActionConstants.MENU_PREFIX + "Admin");
        }
    	MenuManager preferenceMenu = new MenuManager("&Preference", IWorkbenchActionConstants.M_PROJECT_CONFIGURE);
		MenuManager helpMenu = new MenuManager("&Help", IWorkbenchActionConstants.M_HELP);
		
		menuBar.add(fileMenu);
		// Add a group marker indicating where action set menus will appear.
		menuBar.add(new GroupMarker(IWorkbenchActionConstants.MB_ADDITIONS));
		menuBar.add(manageMenu);
		menuBar.add(new GroupMarker(IWorkbenchActionConstants.MB_ADDITIONS));
		if(isAdmin) {
			menuBar.add(adminMenu);
			menuBar.add(new GroupMarker(IWorkbenchActionConstants.MB_ADDITIONS));
		}
		menuBar.add(preferenceMenu);
		menuBar.add(helpMenu);
		
		// File
		fileMenu.add(connectAction);
		fileMenu.add(saveAction);
		fileMenu.add(saveAsAction);
		fileMenu.add(new Separator());
		fileMenu.add(deleteResourceAction);
		fileMenu.add(new Separator());
		fileMenu.add(exitAction);
		
		// Manage
		manageMenu.add(restFulAPIAction);
		manageMenu.add(transactionConnectionAction);
		manageMenu.add(resourceManageAction);
		manageMenu.add(userPermissionAction);
		manageMenu.add(executedSQLAction);
		manageMenu.add(schemaHistoryAction);
		
		if(isAdmin) {
			adminMenu.add(sendMessageAction);
			adminMenu.add(userLoginHistoryAction);
		}

		// preference action
		preferenceMenu.add(preferenceAction);
		
		// Help
		helpMenu.add(bugIssueAction);
		helpMenu.add(aboutAction);
    }
    
    protected void fillCoolBar(ICoolBarManager coolBar) {
        IToolBarManager toolbar = new ToolBarManager(SWT.FLAT | SWT.RIGHT);
        coolBar.add(new ToolBarContributionItem(toolbar, "main"));
        
//        if(PermissionChecker.isDBAShow(SessionManager.getRepresentRole())) {
	        toolbar.add(connectAction);
	        toolbar.add(new Separator());
//        }
        
        toolbar.add(saveAction);
        toolbar.add(saveAsAction);
        toolbar.add(new Separator());        
        
        toolbar.add(queryOpenAction);
        toolbar.add(dbRelationOpenAction);
        toolbar.add(new Separator());
        
//        toolbar.add(deleteResourceAction);
//        toolbar.add(new Separator());
        
//        if(PermissionChecker.isAdmin(SessionManager.getRepresentRole())) {
//	        toolbar.add(sendMessageAction);
//	        toolbar.add(new Separator());
//	        toolbar.add(userLoginHistoryAction);
//	        toolbar.add(new Separator());
//        }
        
//        toolbar.add(restFulAPIAction);
//        toolbar.add(new Separator());
//        
//        toolbar.add(transactionConnectionAction);
//        toolbar.add(new Separator());
//    
//        toolbar.add(resourceManageAction);
//        toolbar.add(new Separator());
//
//    	toolbar.add(userPermissionAction);
//    	toolbar.add(new Separator());
//        
//    	toolbar.add(executedSQLAction);
//        toolbar.add(new Separator());
//        
//        toolbar.add(schemaHistoryAction);
//        toolbar.add(new Separator());
        
//        toolbar.add(preferenceAction);
//        toolbar.add(new Separator());
//        
        toolbar.add(bugIssueAction);
//        toolbar.add(aboutAction);
        
    	toolbar.add(new Separator());
    	toolbar.add(exitAction);
    }
    
}
