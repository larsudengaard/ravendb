﻿namespace Raven.ManagementStudio.UI.Silverlight.Shell
{
	using System;
	using System.ComponentModel.Composition;
	using System.Windows;
	using Caliburn.Micro;
	using Database;
	using Messages;
	using Models;

	[Export(typeof (IShell))]
	public class ShellViewModel : Conductor<DatabaseViewModel>.Collection.OneActive, IShell, IHandle<OpenNewScreen>
	{
		[ImportingConstructor]
		public ShellViewModel(IEventAggregator eventAggregator)
		{
			ActivateItem(new DatabaseViewModel(new Database("http://localhost:8080", "Local")));
			eventAggregator.Subscribe(this);
		}

		public Window Window
		{
			get { return Application.Current.MainWindow; }
		}

		public void Handle(OpenNewScreen message)
		{
			ActiveItem.ActivateItem(message.NewScreen);
		}

		public void CloseWindow()
		{
			Window.Close();
		}

		public void ToogleWindow()
		{
			Window.WindowState = Window.WindowState == WindowState.Normal ? WindowState.Maximized : WindowState.Normal;
		}

		public void MinimizeWindow()
		{
			Window.WindowState = WindowState.Minimized;
		}

		public void DragWindow()
		{
			Window.DragMove();
		}

		public void ResizeWindow(string direction)
		{
			WindowResizeEdge edge;
			Enum.TryParse(direction, out edge);
			Window.DragResize(edge);
		}
	}
}