import psycopg2
import matplotlib.pyplot as plt

class Parallel_step_assignment:
    def __init__(self):
        
        self.table = 'greedy_tgap_tgap'
        self.dbname = "thesis10k"
        self.host = "localhost"
        self.port = "5432"
        self.password = "Dupadupa123"
        self.user = "postgres"

        self.connection = psycopg2.connect(user = self.user, password = self.password, host = self.host, port = self.port, database = self.dbname)
        self.cursor = self.connection.cursor()
        self.create_tables()

    def create_tables(self):

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS {0}_p_face AS 
                            SELECT * 
                            FROM {0}_face 
                            LIMIT 1;
                            TRUNCATE TABLE {0}_p_face;
                            CREATE TABLE IF NOT EXISTS {0}_p_face AS 
                            SELECT * 
                            FROM {0}_face 
                            LIMIT 1;
                            TRUNCATE TABLE {0}_p_face;

                           ;'''.format(self.table))
        self.connection.commit()

    def parallel(self):
        #get the list of face_id in general with step_low>0
        #iterate first
        #list neighbours with step_low<step_low_of_face
        #check if already on list
        #if yes, clean the list, increment step when check again
        #if no, add it to list + neighbours, add parallel step number

        #initiating constants
        flag = False
        locked = {}
        step = 1
        number_polys = 0
        area = 0
        histogram = []
        histogram_area = []

        #parameters
        total_area = 78935448.8792769
        max_area = 0.05

        print('Starting parallel step assignment, fetching tGAP structure to obtain the sequence of generalization…')
        #self.cursor.execute('''SELECT id  FROM {0} WHERE step_high>-1  ORDER BY parallel_step asc'''.format(self.table))
        self.cursor.execute('''SELECT face_id  
                                FROM {0}_face 
                                WHERE step_low>0 
                                ORDER BY step_low ASC'''.format(self.table))
        rows = self.cursor.fetchall()
        print("Sequence acquired successfully, starting assignment with step 1.")

        for row in rows:

            nbrs = self.find_neighbours(row[0])
            if  max_area*total_area <= area and flag == False:#number_polys >= maximum_steps and flag == False:
                    print('Maximum number of operation in one step reached.')
                    flag = True

            for nbr in nbrs:
                if nbr in locked:
                    flag = True
                    
            if flag == False:  
                number_polys += 1
                area += float(self.get_area(row[0]))
                self.create_entry(row, step)
                #print('Object {0} assigned to step {1}, locking neighbors…'.format(row, step))      
                
                for nbr in nbrs:
                    locked[nbr] = nbr

            if flag == True:
                print('Not possible to add object to step {0} ({1} objects assigned), clearing locks and moving to further step...'.format(step, number_polys))

                histogram.append(number_polys)
                histogram_area.append(area)

                step +=1
                number_polys = 1
                locked = {}
                area = float(self.get_area(row[0]))
                flag = False

                for nbr in nbrs:
                    locked[nbr] = nbr   

                self.create_entry(row, step)   

        self.makehistogram(histogram)  
        self.makehistogram(histogram_area)
        print('Low step assigned, starting with the rest!')
        #self.set_step_high()
        return histogram

    def find_neighbours(self,x):
        #input: id of a parent
        #output: neighbours surrounding it (all the scales up to) (list of id)

  

        self.cursor.execute('''SELECT a.face_id 
                                FROM {0}_face a, 
                                    {0}_face b 
                                WHERE ST_Relate(a.geometry, b.geometry,'2********') AND 
                                    a.step_low < b.step_low AND 
                                    b.face_id = {1} AND 
                                    b.face_id <> a.face_id;'''.format(self.table,x))

        rows = self.cursor.fetchall()
        return rows

    def create_entry(self,x, step):
        #create record for given object, note that step_high is not adjusted!
        #print('Entry for object with id = {0} created in step {1}'.format(x, step))
        
        self.cursor.execute('''INSERT INTO {0}_p_face
                                    (face_id, 
                                    imp_low, 
                                    imp_high, 
                                    step_low, 
                                    step_high, 
                                    step_high_sub, 
                                    imp_own, 
                                    area, 
                                    feature_class, 
                                    mbr_geometry, 
                                    pip_geometry, 
                                    geometry)
                                SELECT face_id, 
                                    {1}*2, 
                                    imp_high, 
                                    {1}, 
                                    step_high, 
                                    step_high_sub, 
                                    imp_own, 
                                    area, 
                                    feature_class, 
                                    mbr_geometry, 
                                    pip_geometry, 
                                    geometry
                                FROM {0}_face 
                                WHERE face_id = {2}; '''.format(self.table, step, x[0]))
        self.connection.commit()
        return
    def set_step_high(self):
        #create record for given object, note that step_high is not adjusted!
        #print('Entry for object with id = {0} created in step {1}'.format(x, step)) 

        self.cursor.execute('''DROP TABLE IF EXISTS {0}_done_face;
                            CREATE TABLE {0}_done_face AS
                            SELECT a.face_id, 
                                a.imp_low, 
                                c.step_low*2 as imp_high, 
                                a.step_low, 
                                c.step_low as step_high,
                                c.step_low as step_high_sub, 
                                a.imp_own, 
                                a.area, 
                                a.feature_class, 
                                a.mbr_geometry, 
                                a.pip_geometry, 
                                a.geometry 
                            FROM {0}_p_face a, 
                                {0}_face_hierarchy b, 
                                {0}_p_face c
                            WHERE a.face_id = b.face_id 
                                AND 
                                b.parent_face_id = c.face_id
                            UNION 
                            SELECT a.face_id, 
                                a.imp_low, 
                                1644 as imp_high, 
                                a.step_low, 
                                823 as step_high, 
                                823 as step_high_sub,
                                a.imp_own, 
                                a.area, 
                                a.feature_class, 
                                a.mbr_geometry, 
                                a.pip_geometry, 
                                a.geometry 
                            FROM {0}_p_face a, 
                                {0}_face_hierarchy b
                            WHERE 
                                a.face_id = b.face_id 
                                AND 
                                b.parent_face_id = 0
                            UNION
                            SELECT a.face_id, 
                                a.imp_low, 
                                c.step_low*2 as imp_high, 
                                a.step_low, 
                                c.step_low as step_high,
                                c.step_low as step_high_sub, 
                                a.imp_own, 
                                a.area, 
                                a.feature_class, 
                                a.mbr_geometry, 
                                a.pip_geometry, 
                                a.geometry 
                            FROM {0}_face a, 
                                {0}_face_hierarchy b, 
                                {0}_p_face c
                            WHERE a.face_id = b.face_id 
                                AND 
                                b.parent_face_id = c.face_id
                                AND
                                a.step_low = 0;

                            DROP TABLE IF EXISTS {0}_done_face_hierarchy;
                            CREATE TABLE {0}_done_face_hierarchy AS
                            SELECT a.face_id, 
                                b.imp_low as imp_low, 
                                b.imp_high as imp_high, 
                                b.step_low as step_low, 
                                b.step_high as step_high, 
                                b.step_high_sub as step_high_sub, 
                                a.parent_face_id, 
                                a.if_winner
                            FROM {0}_face_hierarchy a, 
                                {0}_done_face b
                            WHERE a.face_id = b.face_id 
                                AND 
                                a.parent_face_id !=0
                            UNION 
                            SELECT a.face_id, 
                                1644 as imp_low, 
                                1644 as imp_high, 
                                822 as step_low, 
                                823 as step_high, 
                                823 as step_high_sub, 
                                a.parent_face_id, 
                                a.if_winner
                            FROM {0}_face_hierarchy a
                            WHERE a.parent_face_id = 0; 
                            
                            DROP TABLE IF EXISTS {0}_done_edge_low;
                            CREATE TABLE {0}_done_edge_low AS
                            SELECT  
                                a.edge_id, 
                                a.start_node_id, 
                                a.end_node_id, 
                                a.left_face_id_low,
                                a.right_face_id_low,
                                a.left_face_id_high,
                                a.right_face_id_high,
                                b.imp_low as imp_low,
                                a.imp_high,
                                b.step_low as step_low,
                                a.step_high,
                                a.step_high_sub,
                                a.edge_class,
                                a.pickled_blg,
                                a.geometry
                            FROM {0}_edge_link a, 
                                {0}_done_face b
                            WHERE a.face_low = b.face_id 
                                AND 
                                a.face_low !=0
                            UNION 
                            SELECT
                                a.edge_id, 
                                a.start_node_id, 
                                a.end_node_id, 
                                a.left_face_id_low,
                                a.right_face_id_low,
                                a.left_face_id_high,
                                a.right_face_id_high,
                                0 as imp_low,
                                a.imp_high,
                                0 as step_low,
                                a.step_high,
                                a.step_high_sub,
                                a.edge_class,
                                a.pickled_blg,
                                a.geometry
                            FROM {0}_edge_link a 
                            WHERE a.face_low = 0; 

                            DROP TABLE IF EXISTS {0}_done_edge;
                            CREATE TABLE {0}_done_edge AS
                            SELECT 
                                a.edge_id, 
                                a.start_node_id, 
                                a.end_node_id, 
                                a.left_face_id_low,
                                a.right_face_id_low,
                                a.left_face_id_high,
                                a.right_face_id_high,
                                b.imp_low as imp_low,
                                c.imp_high as imp_high,
                                b.step_low as step_low,
                                c.step_high as step_high,
                                c.step_high_sub as step_high_sub,
                                a.edge_class,
                                a.pickled_blg,
                                a.geometry
                            FROM {0}_edge_link a, 
                                {0}_done_face c,
                                {0}_done_edge_low b

                            WHERE a.face_high = c.face_id 
                                AND
                                a.edge_id = b.edge_id ;'''.format(self.table))
        
        self.connection.commit()
        print('Done!')


    def makehistogram(self,h):
        #draw histogram

        steps  = []
        for i in range(len(h)):
            steps.append(i+1)

        plt.bar(steps,h)
        plt.xlabel('Step (nr)')
        plt.ylabel('Operations (no)')
        plt.show()

    def get_area(self,id):
        #returns area of given polygon 
        self.cursor.execute('''SELECT ST_Area(geometry) 
                                FROM {0}_face 
                                WHERE face_id = {1};'''.format(self.table,id))

        area = self.cursor.fetchall()
        return area[0][0]


if __name__ == "__main__":
    a = Parallel_step_assignment()
    a.parallel()

