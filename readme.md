Using the tables 'page' and 'imagelinks' from en-wiki, 
we are going to do a join and get the pairs:
image_name	page_name

To create the JAR:
In the project in Eclipse, create a folder named dist, 
right click on build.xml, then Run As ..., then make sure 
dist is clicked and hit Run. Refresh the project (F5) and 
make sure you have the JAR file.

Then you should copy the JAR file and both dumps to the 
server.

Now you just need to call the Hadoop job with the command:
hadoop jar [path]/mdp-lab4.jar [path]/enwiki-20151201-page.sql.gz [path]/enwiki-20151201-imagelinks.sql.gz [path]/output