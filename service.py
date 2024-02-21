import os
import shutil
import tempfile
import zipfile
from PIL import Image

from waitress import serve
from flask_cors import CORS
from flask import Flask, send_file

from werkzeug.utils import secure_filename
from flask_restplus import Resource, Api, reqparse
from werkzeug.datastructures import FileStorage

from config import threshold
from db_driver import login, create_new_user, modify_password, if_table_exists, create_lion_data_table, \
    create_user_data_table, truncate_table, drop_table, get_lion_name_info, get_lion_id_info, get_data, \
    update_lion_microchip_number_parameter, update_user_parameter, delete_user, delete_lion_name, delete_lion_id, \
    delete_veterinary_history, get_current_count, \
    get_all_lions, get_lion_parameter,get_user_info, admin_reset_password, get_all_lion_embeddings, \
    get_lion_gender_info, get_lion_status_info, get_lion_page, get_lion_by_filter, get_lion_gender_status, get_lion_nml, \
    get_lion_id_name, get_lion_name_gender, get_lion_microchip_number_info, get_lion_id_microchip_number, \
    get_lion_name_microchip_number, \
    get_lion_id_gender, get_lion_id_status, get_lion_name_status, get_lion_id_gender_status, get_lion_id_name_status, \
    get_lion_id_name_gender, get_lion_id_name_gender_microchip_number, get_lion_details_info, \
    veterinary_health_details, get_lion_id_page, create_veterinary_history, update_veterinary_history, \
    lion_identificaion, generate_report

from utils import on_board_new_lion, current_milli_time, check_upload, upload_one_lion
from compressed_Table import get_all_compressed_lions, create_compressed_table, get_all_compressed_faces

import traceback


def store_and_verify_file(file_from_request, work_dir):
    if not file_from_request.filename:
        return -1, 'Empty file part provided!'
    try:
        file_path = os.path.join(work_dir, secure_filename(file_from_request.filename))
        if os.path.exists(file_path) is False:
            file_from_request.save(file_path)
        return 0, file_path
    except Exception as ex:
        return -1, str(ex)


def upload_and_verify_file(file_from_request, work_dir):
    if not file_from_request.filename:
        return -1, 'Empty file part provided!', None
    try:
        fn = str(current_milli_time()) + '_' + secure_filename(file_from_request.filename)
        file_path = os.path.join(work_dir, fn)
        if os.path.exists(file_path) is False:
            file_from_request.save(file_path)
        return 0, file_path, fn
    except Exception as ex:
        return -1, str(ex), None

def png_to_jpeg_converter(dir_path):
    base_path=dir_path
    for infile in os.listdir(base_path):
        if infile[-3:] == "png":
            # print(infile)
            img_path=base_path+'/'+infile.split('.')[0] + '.png'
            outfile=dir_path+'/'+infile.split('.')[0] + '.jpg'
            print("outfile: ",outfile)

            im = Image.open(img_path)
            if not im.mode == 'RGB':
                im = im.convert('RGB')
            im.save(outfile)
            os.remove(img_path)



def init():
    if not if_table_exists(table_name='user_data'):
        create_user_data_table()
    if not if_table_exists(table_name='lion_data'):
        create_lion_data_table()
    if not if_table_exists(table_name='compressed_images'):
        create_compressed_table()



def create_app():
    init()
    app = Flask("foo", instance_relative_config=True)

    api = Api(
        app,
        version='1.0.0',
        title='TelioLabs Lion Backend App',
        description='TelioLabs Lion Backend App',
        default='TelioLabs Lion Backend App',
        default_label=''
    )

    CORS(app)

    @api.route('/get_all_lions')
    class GetAllLionsService(Resource):
        @api.doc(responses={"response": 'json'})
        def get(self):
            try:
                #ret, r = get_all_lions()
                ret,r = get_all_compressed_lions()
                if r == 0:
                    return ret, 200
                else:
                    return ret, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    @api.route('/get_count')
    class GetCountService(Resource):
        @api.doc(responses={"response": 'json'})
        def get(self):
            try:
                ret, r = get_current_count()
                if r == 0:
                    return ret, 200
                else:
                    return ret, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    adjust_threshold_parser = reqparse.RequestParser()
    adjust_threshold_parser.add_argument('sign',
                                         type=str,
                                         help='The sign of delta change in threshold.',
                                         required=True)
    adjust_threshold_parser.add_argument('delta',
                                         type=str,
                                         help='The delta change in threshold.',
                                         required=True)

    @api.route('/adjust_threshold')
    @api.expect(adjust_threshold_parser)
    class AdjustThresholdService(Resource):
        @api.expect(adjust_threshold_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = adjust_threshold_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                delta_str = args['delta']
                sign_str = args['sign']
                threshold.set_threshold(sign_str, delta_str)
                print("sign = " + sign_str)
                print("delta = " + delta_str)
                rv = dict()
                rv['status'] = 'Success'
                return rv, 200
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    upload_parser = reqparse.RequestParser()
    upload_parser.add_argument('instance_file',
                               location='files',
                               type=FileStorage,
                               help='The instance file to be uploaded.',
                               required=True)
    upload_parser.add_argument('name',
                               type=str,
                               help='The name of the lion.',
                               required=True)
    upload_parser.add_argument('microchip_number',
                               type=str,
                               help='The id similar to another lion (optional).',
                               required=False)

    upload_parser.add_argument('age',
                               type=int,
                               help='The age of the lion.',
                               required=False)
    upload_parser.add_argument('gender',
                               type=str,
                               help='The gender of the lion.',
                               required=False)
    upload_parser.add_argument('status',
                               type=str,
                               help='The status of the lion.',
                               required=False)

    @api.route('/upload')
    @api.expect(upload_parser)
    class UploadService(Resource):
        @api.expect(upload_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = upload_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                temp_dir = tempfile.mkdtemp()
                file_from_request = args['instance_file']
                ret, status_file_path = store_and_verify_file(file_from_request, temp_dir)
                if ret != 0:
                    rv = dict()
                    rv['status'] = status_file_path
                    return rv, 404
                name = args['name']
                microchip_number =args['microchip_number']
                try:
                    age = args['age']
                    if age is None:
                        age = ''
                except Exception as e:
                    age = ''
                try:
                    gender = args['gender']
                    if gender is None:
                        gender = ''
                except Exception as e:
                    gender = ''
                try:
                    status = args['status']
                    if gender is None:
                        status = ''
                except Exception as e:
                    status = ''
                # if len(name) == 0 or len(_id) == 0:
                #     rv = dict()
                #     rv['status'] = "both are empty"
                #     return rv, 404
                # if len(name) == 0:
                #     name, ret = get_lion_parameter(_id, 'name')
                #     if ret != 0:
                #         rv = dict()
                #         rv['status'] = "no name associated with id"
                #         return rv, 404
                rv = upload_one_lion(status_file_path, name,microchip_number, gender, status, age)
                return rv, 200
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
    delete_lion_id_parser = reqparse.RequestParser()
    delete_lion_id_parser.add_argument('username',
                                       type=str,
                                       help='An admin user name',
                                       required=True)
    delete_lion_id_parser.add_argument('lion_id',
                                       type=str,
                                       help='The instance of lion id to be deleted',
                                       required=True)

    @api.route('/delete_lion_id')
    @api.expect(delete_lion_id_parser)
    class DeleteLionIDService(Resource):
        @api.expect(delete_lion_id_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = delete_lion_id_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            try:
                username = args['username']
                lion_id = args['lion_id']
                ret_str, ret = delete_lion_id(username, lion_id)
                rv = dict()
                rv['status'] = ret_str
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    delete_lion_name_parser = reqparse.RequestParser()
    delete_lion_name_parser.add_argument('username',
                                         type=str,
                                         help='An admin user name',
                                         required=True)
    delete_lion_name_parser.add_argument('lion_name',
                                         type=str,
                                         help='All the instances of lion name to be deleted',
                                         required=True)

    @api.route('/delete_lion_name')
    @api.expect(delete_lion_name_parser)
    class DeleteLionNameService(Resource):
        @api.expect(delete_lion_name_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = delete_lion_name_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            try:
                username = args['username']
                lion_name = args['lion_name']
                ret_str, ret = delete_lion_name(username, lion_name)
                rv = dict()
                rv['status'] = ret_str
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    delete_user_parser = reqparse.RequestParser()
    delete_user_parser.add_argument('username1',
                                    type=str,
                                    help='In case an admin is deleting username2, '
                                         'then the admin username, else optional',
                                    required=False)
    delete_user_parser.add_argument('username2',
                                    type=str,
                                    help='The username to be deleted',
                                    required=True)
    delete_user_parser.add_argument('password2',
                                    type=str,
                                    help='The password of the username to be deleted, if admin then optional',
                                    required=False)

    @api.route('/delete_user')
    @api.expect(delete_user_parser)
    class DeleteUserService(Resource):
        @api.expect(delete_user_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = delete_user_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            try:
                try:
                    username1 = args['username1']
                    if username1 is None:
                        username1 = ''
                except Exception as e:
                    username1 = ''
                username2 = args['username2']
                try:
                    password2 = args['password2']
                    if password2 is None:
                        password2 = ''
                except Exception as e:
                    password2 = ''
                ret_str, ret = delete_user(username1, username2, password2)
                rv = dict()
                rv['status'] = ret_str
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    edit_user_data_parser = reqparse.RequestParser()
    # edit_user_data_parser.add_argument('who',
    #                                    type=str,
    #                                    help='Who is changing the user data.',
    #                                    required=True)
    edit_user_data_parser.add_argument('whose',
                                       type=str,
                                       help='Whose user data is being changed.',
                                       required=True)
    edit_user_data_parser.add_argument('param_name',
                                       type=str,
                                       help='The param name - name or email or phone or role',
                                       required=True)

    edit_user_data_parser.add_argument('param_value',
                                       type=str,
                                       help='The param value',
                                       required=True)
    # edit_user_data_parser.add_argument('password',
    #                                    type=str,
    #                                    help='The password of whose (Optional, if who is admin) ',
    #                                    required=False)

    @api.route('/edit_user_data')
    @api.expect(edit_user_data_parser)
    class EditUserDataService(Resource):
        @api.expect(edit_user_data_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = edit_user_data_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            try:
                # who = args['who']
                whose = args['whose']
                param_name = args['param_name']
                param_value = args['param_value']
                # try:
                #     password = args['password']
                #     if password is None:
                #         password = ''
                # except Exception as e:
                #     password = ''
                ret_str, ret = update_user_parameter( whose, param_name, param_value)
                rv = dict()
                rv['status'] = ret_str
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    edit_lion_data_parser = reqparse.RequestParser()
    edit_lion_data_parser.add_argument('lion_microchip',
                                       type=str,
                                       help='The lion Microchip No',
                                       required=True)
    edit_lion_data_parser.add_argument('lion_status',
                                       type=str,
                                       help='The lion status, A - Alive or D - Dead',
                                       required=True)
    edit_lion_data_parser.add_argument('lion_gender',
                                       type=str,
                                       help='The lion sex, M - Male or F - Female or U - Unknown',
                                       required=True)

    @api.route('/edit_lion_data')
    @api.expect(edit_lion_data_parser)
    class EditLionDataService(Resource):
        @api.expect(edit_lion_data_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = edit_lion_data_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                lion_microchip = args['lion_microchip']
                lion_status = args['lion_status']
                lion_gender = args['lion_gender']
                ret_str, ret_status = update_lion_microchip_number_parameter(lion_microchip, 'status', lion_status)
                ret_str, ret_sex = update_lion_microchip_number_parameter(lion_microchip, 'sex', lion_gender)
                rv = dict()
                rv['status'] = ret_str
                if ret_status == 0 and ret_sex == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    get_user_info_parser = reqparse.RequestParser()
    get_user_info_parser.add_argument('username',
                                      type=str,
                                      help='The user name',
                                      required=True)

    @api.route('/get_user_info')
    @api.expect(get_user_info_parser)
    class GetUserInfoService(Resource):
        @api.expect(get_user_info_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = get_user_info_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                _username = args['username']
                rv, ret = get_user_info(_username)
                if ret != 0:
                    return rv, 404
                else:
                    return rv, 200
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    get_parser = reqparse.RequestParser()
    get_parser.add_argument('offset',
                            type=int,
                            help='Offset from which records needs to be read',
                            required=True)
    get_parser.add_argument('count',
                            type=int,
                            help='Number of records to be read from offset',
                            required=True)
    get_parser.add_argument('loggedinuser',
                            type=str,
                            help='Name of logged-In user',
                            required=True)

    @api.route('/list')
    @api.expect(get_parser)
    class ListService(Resource):
        @api.expect(get_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = get_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            try:
                offset = args['offset']
                count = args['count']
                loggedinuser = args['loggedinuser']
                rv, ret = get_data(offset, count, loggedinuser)
                if ret != 0:
                    return rv, 404
                else:
                    return rv, 200
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    get_lion_id_info_parser = reqparse.RequestParser()
    get_lion_id_info_parser.add_argument('lion_id',
                                         type=str,
                                         help='The lion id',
                                         required=True)

    @api.route('/get_lion_id_info')
    @api.expect(get_lion_id_info_parser)
    class GetLionIDInfoService(Resource):
        @api.expect(get_lion_id_info_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = get_lion_id_info_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                lion_id = args['lion_id']
                rv, ret = get_lion_id_info(lion_id)
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    get_lion_name_info_parser = reqparse.RequestParser()
    get_lion_name_info_parser.add_argument('lion_name',
                                           type=str,
                                           help='The lion name',
                                           required=True)

    @api.route('/get_lion_name_info')
    @api.expect(get_lion_name_info_parser)
    class GetLionNameInfoService(Resource):
        @api.expect(get_lion_name_info_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = get_lion_name_info_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                lion_name = args['lion_name']
                rv, ret = get_lion_name_info(lion_name)
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    check_upload_parser = reqparse.RequestParser()
    check_upload_parser.add_argument('payload',
                                     location='files',
                                     type=FileStorage,
                                     help='A zip of lion images',
                                     required=True)

    @api.route('/check_upload')
    @api.expect(check_upload_parser)
    class CheckUploadService(Resource):
        @api.expect(check_upload_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = check_upload_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            extract_dir = None
            download_dir = None
            try:
                file_from_request = args['payload']
                extract_dir = tempfile.mkdtemp()
                download_dir = tempfile.mkdtemp()
                ret, file_path_or_status = store_and_verify_file(file_from_request, download_dir)
                if ret == 0:
                    zip_handle = zipfile.ZipFile(file_path_or_status, "r")
                    zip_handle.extractall(path=extract_dir)
                    zip_handle.close()
                    _payload_dir = os.path.join(extract_dir, 'images')
                    png_to_jpeg_converter(_payload_dir)


                    _lion_images = os.listdir(_payload_dir)
                    rv = dict()
                    rv['status'] = []
                    for _lion_image in _lion_images:
                        _lion_image_path = os.path.join(_payload_dir, _lion_image)
                        ret = check_upload(_lion_image_path)
                        rv['status'].append({'image': _lion_image, 'ret': ret})
                    if extract_dir:
                        shutil.rmtree(extract_dir)
                    if download_dir:
                        shutil.rmtree(download_dir)
                    return rv, 200
                else:
                    rv = dict()
                    rv['status'] = file_path_or_status
                    if extract_dir:
                        shutil.rmtree(extract_dir)
                    if download_dir:
                        shutil.rmtree(download_dir)
                    return rv, 404
            except Exception as e:
                if extract_dir:
                    shutil.rmtree(extract_dir)
                if download_dir:
                    shutil.rmtree(download_dir)
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    onboard_parser = reqparse.RequestParser()
    onboard_parser.add_argument('payload',
                                location='files',
                                type=FileStorage,
                                help='A zip of dirs, where each dir name is a lion name and '
                                     'each dir content is a set of lion images',
                                required=True)



    @api.route('/on_board_new_lions')
    @api.expect(onboard_parser)
    class OnboardService(Resource):
        @api.expect(onboard_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = onboard_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            extract_dir = None
            download_dir = None
            try:
                file_from_request = args['payload']
                extract_dir = tempfile.mkdtemp()
                download_dir = tempfile.mkdtemp()
                ret, file_path_or_status = store_and_verify_file(file_from_request, download_dir)
                if ret == 0:
                    zip_handle = zipfile.ZipFile(file_path_or_status, "r")
                    zip_handle.extractall(path=extract_dir)
                    zip_handle.close()
                    _payload_dir = os.path.join(extract_dir, 'lions')
                    _lions = os.listdir(_payload_dir)
                    print("_lion",_lions)
                    path = _payload_dir
                    input_list = os.listdir(path)
                    for each_lion in input_list:
                        splitted_name = each_lion.split('#')
                        microchip_number = splitted_name[0]
                        if len(splitted_name)==2:
                            lion_name = splitted_name[1]
                        else:
                            lion_name =''
                        lion_identificaion(microchip_number,lion_name)
                    for folder in os.listdir(path):
                        basepath = os.path.join(path, folder)

                    for folder in os.listdir(path):
                        basepath = os.path.join(path, folder)

                        png_to_jpeg_converter(basepath)


                    rv = dict()
                    rv['status'] = []
                    embeddings = get_all_lion_embeddings()
                    if len(embeddings) > 0:
                        second = True
                    else:
                        second = False
                    for _lion in _lions:
                        splitted_name = _lion.split('#')
                        microchip_number = splitted_name[0]
                        if len(splitted_name)==2:
                            lion_name = splitted_name[1]
                        else:
                            lion_name =''
                        _lion_dir = os.path.join(_payload_dir, _lion)
                        on_board_new_lion(lion_name,microchip_number, _lion_dir, rv, second)
                    if extract_dir:
                        shutil.rmtree(extract_dir)
                    if download_dir:
                        shutil.rmtree(download_dir)
                    return rv, 200
                else:
                    rv = dict()
                    rv['status'] = file_path_or_status
                    if extract_dir:
                        shutil.rmtree(extract_dir)
                    if download_dir:
                        shutil.rmtree(download_dir)
                    return rv, 404
            except Exception as e:
                if extract_dir:
                    shutil.rmtree(extract_dir)
                if download_dir:
                    shutil.rmtree(download_dir)
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    drop_table_parser = reqparse.RequestParser()
    drop_table_parser.add_argument('table_name',
                                   type=str,
                                   help='The Table to be destroyed/dropped',
                                   required=True)

    @api.route('/drop_table')
    @api.expect(drop_table_parser)
    class DropTableService(Resource):
        @api.expect(drop_table_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = drop_table_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                table_name = args['table_name']
                if if_table_exists(table_name=table_name):
                    ret, status = drop_table(table_name)
                else:
                    status = table_name + " doesn't exist!"
                    ret = -1
                rv = dict()
                rv['status'] = status
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    truncate_table_parser = reqparse.RequestParser()
    truncate_table_parser.add_argument('table_name',
                                       type=str,
                                       help='The Table to be truncated',
                                       required=True)

    @api.route('/truncate_table')
    @api.expect(truncate_table_parser)
    class TruncateTableService(Resource):
        @api.expect(truncate_table_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = truncate_table_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                table_name = args['table_name']
                if if_table_exists(table_name=table_name):
                    ret, status = truncate_table(table_name)
                else:
                    status = table_name + " doesn't exist!"
                    ret = -1
                rv = dict()
                rv['status'] = status
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    user_login_parser = reqparse.RequestParser()
    user_login_parser.add_argument('un',
                                   type=str,
                                   help='User Name',
                                   required=True)
    user_login_parser.add_argument('pw',
                                   type=str,
                                   help='Password',
                                   required=True)

    @api.route('/user_login')
    @api.expect(user_login_parser)
    class UserLoginService(Resource):
        @api.expect(user_login_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = user_login_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                _un = args['un']
                _pw = args['pw']
                ret, role = login(_un, _pw)
                rv = dict()
                if ret is True:
                    rv['status'] = "Login Success"
                    rv['un'] = _un
                    rv['role'] = role
                    return rv, 200
                else:
                    rv['status'] = "Invalid Credentials"
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    create_new_user_parser = reqparse.RequestParser()
    create_new_user_parser.add_argument('name',
                                        type=str,
                                        help='Name',
                                        required=True)
    create_new_user_parser.add_argument('email',
                                        type=str,
                                        help='Email of user',
                                        required=True)
    create_new_user_parser.add_argument('phone',
                                        type=str,
                                        help='Phone number of user',
                                        required=True)
    create_new_user_parser.add_argument('role',
                                        type=str,
                                        help='Roles - admin, user',
                                        required=True)
    create_new_user_parser.add_argument('un',
                                        type=str,
                                        help='User Name',
                                        required=True)

    @api.route('/create_new_user')
    @api.expect(create_new_user_parser)
    class CreateNewUserService(Resource):
        @api.expect(create_new_user_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = create_new_user_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                _name = args['name']
                _email = args['email']
                _phone = args['phone']
                _role = args['role']
                _un = args['un']
                pwd, ret, status = create_new_user(_name, _email, _phone, _role, _un)
                rv = dict()
                rv['status'] = status
                rv['password'] = pwd
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    admin_reset_password_parser = reqparse.RequestParser()
    admin_reset_password_parser.add_argument('admin_username',
                                             type=str,
                                             help='An Admin User Name',
                                             required=True)
    admin_reset_password_parser.add_argument('admin_password',
                                             type=str,
                                             help='The Admin Password',
                                             required=True)
    admin_reset_password_parser.add_argument('username',
                                             type=str,
                                             help='The user whose password has to be reset.',
                                             required=True)

    @api.route('/admin_reset_password')
    @api.expect(admin_reset_password_parser)
    class AdminResetPasswordService(Resource):
        @api.expect(admin_reset_password_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = admin_reset_password_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                _admin_username = args['admin_username']
                _admin_password = args['admin_password']
                _username = args['username']
                status_or_pwd, ret = admin_reset_password(_admin_username, _admin_password, _username)
                rv = dict()
                rv['status'] = 'Success'
                rv['Password'] = status_or_pwd
                if ret == 0:
                    return rv, 200
                else:
                    rv = dict()
                    rv['status'] = status_or_pwd
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    modify_password_parser = reqparse.RequestParser()
    modify_password_parser.add_argument('un',
                                        type=str,
                                        help='User Name',
                                        required=True)
    modify_password_parser.add_argument('old_pw',
                                        type=str,
                                        help='Old Password',
                                        required=True)
    modify_password_parser.add_argument('new_pw',
                                        type=str,
                                        help='New Password',
                                        required=True)

    @api.route('/modify_password')
    @api.expect(modify_password_parser)
    class ModifyPasswordService(Resource):
        @api.expect(modify_password_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            # try:
            #     args = modify_password_parser.parse_args()
            # except Exception as e:
            #     rv = dict()
            #     rv['status'] = str(e)
            #     return rv, 404
            args = modify_password_parser.parse_args()
            try:
                _un = args['un']
                _old_pw = args['old_pw']
                _new_pw = args['new_pw']
                ret, status = modify_password(_un,_old_pw,_new_pw)
                rv = dict()
                rv['status'] = status
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    search_parser = reqparse.RequestParser()

    search_parser.add_argument('lion_id',
                               type=str,
                               help='The lion id',
                               required=False)
    search_parser.add_argument('microchip_number',
                               type=str,
                               help='The microchip number',
                               required=False)
    search_parser.add_argument('lion_name',
                               type=str,
                               help='The lion name',
                               required=False)

    search_parser.add_argument('lion_gender',
                               type=str,
                               help='The lion gender',
                               required=False)
    search_parser.add_argument('lion_status',
                               type=str,
                               help='The lion status ',
                               required=False)
    # Pagination

    search_parser.add_argument('page_number',
                                      type=int,
                                      help='Page Number in UI',
                                      required=True)

    search_parser.add_argument('limit',
                                type=int,
                                help='limit of db',
                                required=True)

    @api.route('/SearchByFilter')
    @api.expect(search_parser)
    class Search_byfilter(Resource):
        @api.expect(search_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = search_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

            try:
                page_number = args['page_number']
                limit = args['limit']
                id = args['lion_id']
                try:
                    if id is None:
                        id = 0
                except Exception  as e:
                    id = 0
                microchip_number = args['microchip_number']
                try:
                    if microchip_number is None:
                        microchip_number = 0
                except Exception  as e:
                    microchip_number = 0
                name = args['lion_name']
                try :
                    if name is None:
                        name = 0
                except Exception as e:
                    name = 0

                _status = args['lion_status']
                try:
                    if _status is None:
                        _status = 0
                except Exception as e:
                    _status = 0

                gender = args['lion_gender']
                try:
                    if gender is None:
                        gender = 0
                except Exception as e:
                    gender = 0

                if id and (not(name)) and (not(microchip_number)) and (not(_status)) and (not(gender)):
                    rv, ret =get_lion_id_page(id,page_number,limit)
                    if ret == 0:
                        return rv,200
                    else:
                        return rv,404

                if gender and (not(name)) and (not(microchip_number))and (not(_status)) and (not(id)):
                    rv, ret = get_lion_gender_info(gender,page_number,limit)

                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404

                if _status and (not(name))and (not(microchip_number)) and (not(gender)) and (not(id)):
                    rv, ret = get_lion_status_info(_status,page_number,limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404



                if name and (not(id)) and (not(microchip_number))and (not(_status)) and (not(gender)):
                    rv, ret = get_lion_name_info(name,page_number,limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404

                if microchip_number and (not (id)) and (not (name)) and (not (_status)) and (not (gender)):
                    rv, ret = get_lion_microchip_number_info(microchip_number, page_number, limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404

                if microchip_number and (not(id)) and (not(name)) and (not(_status)) and (not (gender)):


                if id and name and (not(microchip_number)) and (not(gender)) and (not(_status)):
                    rv,ret = get_lion_id_name(id,name,page_number,limit)
                    if ret == 0:
                        return rv,200
                    else:
                        return rv,404

                if id and gender and (not(microchip_number)) and (not(_status)) and (not(name)):
                    rv,ret = get_lion_id_gender(id,gender,page_number,limit)
                    if ret == 0:
                        return rv,200
                    else:
                        return rv,404

                if id and _status  and (not(microchip_number))and (not(gender)) and (not(name)):
                    rv, ret = get_lion_id_status(id,_status,page_number,limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404


                if id and _status and (not(microchip_number)) and (not(gender)) and (not(name)):
                    rv, ret = get_lion_id_status(id, _status, page_number, limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404





                if id and microchip_number  and (not(_status))and (not(gender)) and (not(name)):
                    rv, ret = get_lion_id_microchip_number(id,microchip_number,page_number,limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404

                if name and gender and (not(microchip_number)) and (not(_status)) and (not(id)):
                    rv,ret = get_lion_name_gender(name,gender,page_number,limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404

                if name and gender and (not (microchip_number)) and (not (_status)) and (not (id)):
                    rv, ret = get_lion_id_name_gender(name,gender,page_number,limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404


                if name and _status and (not(microchip_number)) and (not(gender)) and (not(id)):
                    rv,ret = get_lion_name_status(name,_status,page_number,limit)
                    if ret == 0:
                        return rv,200
                    else:
                        return rv,404

                if gender and _status and (not(microchip_number))and (not(name)) and (not(id)):
                    rv, ret = get_lion_gender_status(gender,_status,page_number,limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv,404

                if id and gender and _status and (not(microchip_number)) and (not(name)):
                        rv,ret = get_lion_id_gender_status(id,gender,_status,page_number,limit)
                        if ret == 0:
                            return rv ,200
                        else:
                            return rv,404

                if id and _status and name and (not(microchip_number))and (not(gender)):
                    rv,ret = get_lion_id_name_status(id,_status,name,page_number,limit)
                    if ret == 0:
                        return rv ,200
                    else:
                        return rv,404

                if name and gender and _status and (not(microchip_number))and (not(id)):
                    rv,ret = get_lion_nml(name,gender,_status,page_number,limit)
                    if ret == 0:
                        return rv ,200
                    else:
                        return rv,404

                if id and gender and name and (not(microchip_number))and (not(_status)):
                    rv,ret = get_lion_id_name_gender(id,gender,name,page_number,limit)
                    if ret == 0:
                        return rv ,200
                    else:
                        return rv,404

                if id and gender and name and microchip_number  and (not(_status)):
                    rv,ret = get_lion_id_name_gender_microchip_number(id,gender,name,microchip_number,page_number,limit)
                    if ret == 0:
                        return rv ,200
                    else:
                        return rv,404
                if id and gender and _status and microchip_number and (not (name)):
                    rv, ret = get_lion_id_name_gender(id, gender, _status,microchip_number, page_number, limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404
                if id and gender and _status and microchip_number and (not (name)):
                    rv,ret = get_lion_id_name_gender(id, gender, _status,microchip_number,page_number,limit)
                    if ret == 0:
                        return rv,200
                    else:
                        return rv, 404
                if id and gender and _status and microchip_number and (not(name)):
                    rv , ret =get_lion_id_name_gender(id,gender , _status,microchip_number, page_number,limit)
                    if ret == 0:
                        return rv ,200
                    else :
                        return rv ,404
                if id and name and _status and microchip_number and (not (gender)):
                    rv, ret = get_lion_id_name_gender(id, name, _status, microchip_number, page_number, limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404
                if id and name and gender and _status:
                    rv, ret=get_lion_by_filter(id,name,gender,_status,page_number,limit)
                    if ret == 0:
                        return rv, 200
                    else:
                        return rv, 404

                if id and name and gender and _status:
                    rv, ret = get_lion_id_name_gender(id, name ,gender,_status,page_number, limit)
                    if ret ==0:
                        return rv ,200
                    else:
                        rv ,404


            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404


    #Pagination
    get_lion_page_parser = reqparse.RequestParser()
    get_lion_page_parser.add_argument('page_number',
                                      type=int,
                                      help='Page Number in UI',
                                      required=True)

    get_lion_page_parser.add_argument('limit',
                                      type=int,
                                      help='limit of db',
                                      required=True)



    @api.route('/get_lion_page')
    @api.expect(get_lion_page_parser)
    class GetLionPageService(Resource):
        @api.expect(get_lion_page_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = get_lion_page_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                page_number = args['page_number']
                limit = args['limit']

                rv, ret = get_lion_page(page_number, limit)
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404


    get_lion_details_page_parser = reqparse.RequestParser()

    get_lion_details_page_parser.add_argument('lion_name',
                                              type=str,
                                              help='lion name',
                                              required=True)

    get_lion_details_page_parser.add_argument('page_number',
                                              type=int,
                                              help='Page Number in UI',
                                              required=True)

    get_lion_details_page_parser.add_argument('limit',
                                              type=int,
                                              help='limit to show number of rows in UI',
                                              required=True)

    # get lion details info pagination
    @api.route('/get_lion_details_page')
    @api.expect(get_lion_details_page_parser)
    class GetLionPageService(Resource):
        @api.expect(get_lion_details_page_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = get_lion_details_page_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                page_number = args['page_number']
                limit = args['limit']
                lion_name = args['lion_name']

                rv, ret = get_lion_details_info(lion_name, page_number, limit)
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    # Pagination in popup
    get_all_compressed_faces_parser = reqparse.RequestParser()
    get_all_compressed_faces_parser.add_argument('page_number',
                                                     type=int,
                                                     help='Page Number in UI',
                                                     required=True)

    get_all_compressed_faces_parser.add_argument('limit',
                                                     type=int,
                                                     help='limit of db',
                                                     required=True)

    @api.route('/get_all_compressed_faces')
    class GetAllCompressedFaces(Resource):
        @api.expect(get_all_compressed_faces_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = get_all_compressed_faces_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                page_number = args['page_number']
                limit = args['limit']

                rv, ret = get_all_compressed_faces(page_number, limit)
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    #create Veterinary table
    
    create_veterinary_history_parser = reqparse.RequestParser()
    create_veterinary_history_parser.add_argument('lion_id',
                                                  type=int,
                                                  help='Lion_id',
                                                  required=True)
    create_veterinary_history_parser.add_argument('microchip_number',
                                                  type=str,
                                                  help='microchip_number',
                                                  required=False)
    create_veterinary_history_parser.add_argument('approx_age',
                                                  type=str,
                                                  help='approx_age',
                                                  required=False)
    create_veterinary_history_parser.add_argument('category_based_on_age',
                                                  type=str,
                                                  help='category_based_on_age',
                                                  required=False)
    create_veterinary_history_parser.add_argument('sex',
                                                  type=str,
                                                  help='sex',
                                                  required=False)
    create_veterinary_history_parser.add_argument('if_female_lactating_or_not',
                                                  type=str,
                                                  help='if_female_lactating_or_not',
                                                  required=False)
    create_veterinary_history_parser.add_argument('radio_collar_no',
                                                  type=str,
                                                  help='radio_collar_no',
                                                  required=False)
    create_veterinary_history_parser.add_argument('frequency_on_reciever',
                                                  type=str,
                                                  help='frequency_on_reciever',
                                                  required=False)
    create_veterinary_history_parser.add_argument('date_of_radio_collaring',
                                                  type=str,
                                                  help='date_of_radio_collaring',
                                                  required=False)
    create_veterinary_history_parser.add_argument('place_of_radio_collaring',
                                                  type=str,
                                                  help='place_of_radio_collaring',
                                                  required=False)
    create_veterinary_history_parser.add_argument('date_of_rescue',
                                                  type=str,
                                                  help='date_of_rescue',
                                                  required=False)
    create_veterinary_history_parser.add_argument('place_of_rescue',
                                                  type=str,
                                                  help='place_of_rescue',
                                                  required=False)
    create_veterinary_history_parser.add_argument('reason_of_rescue',
                                                  type=str,
                                                  help='reason_of_rescue',
                                                  required=False)
    create_veterinary_history_parser.add_argument('date_of_release',
                                                  type=str,
                                                  help='date_of_release',
                                                  required=False)
    create_veterinary_history_parser.add_argument('place_of_release',
                                                  type=str,
                                                  help='place_of_release',
                                                  required=False)
    create_veterinary_history_parser.add_argument('captivity_details',
                                                  type=str,
                                                  help='captivity_details',
                                                  required=False)
    create_veterinary_history_parser.add_argument('clinical_observations',
                                                  type=str,
                                                  help='clinical_observations',
                                                  required=False)
    create_veterinary_history_parser.add_argument('veterinary_treatment_or_prophylaxis',
                                                  type=str,
                                                  help='veterinary_treatment_or_prophylaxis',
                                                  required=False)
    create_veterinary_history_parser.add_argument('date_of_death',
                                                  type=str,
                                                  help='date_of_death',
                                                  required=False)
    create_veterinary_history_parser.add_argument('place_of_death',
                                                  type=str,
                                                  help='place_of_death',
                                                  required=False)
    create_veterinary_history_parser.add_argument('reason_of_death',
                                                  type=str,
                                                  help='reason_of_death',
                                                  required=False)
    create_veterinary_history_parser.add_argument('remarks',
                                                  type=str,
                                                  help='remarks',
                                                  required=False)
    @api.route('/create_veterinary_history')
    @api.expect(create_veterinary_history_parser)
    class CreateNewMedicalHistory(Resource):
        @api.expect(create_veterinary_history_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = create_veterinary_history_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                _lion_id = args['lion_id']
                _microchip_number = args['microchip_number']
                _approx_age = args['approx_age']
                _category_based_on_age = args['category_based_on_age']
                _sex = args['sex']
                _if_female_lactating_or_not = args['if_female_lactating_or_not']
                _radio_collar_no = args['radio_collar_no']
                _frequency_on_reciever = args['frequency_on_reciever']
                _date_of_radio_collaring = args['date_of_radio_collaring']
                _place_of_radio_collaring = args['place_of_radio_collaring']
                _date_of_rescue = args['date_of_rescue']
                _place_of_rescue = args['place_of_rescue']
                _reason_of_rescue = args['reason_of_rescue']
                _date_of_release = args['date_of_release']
                _place_of_release = args['place_of_release']
                _captivity_details = args['captivity_details']
                _clinical_observations = args['clinical_observations']
                _veterinary_treatment_or_prophylaxis = args['veterinary_treatment_or_prophylaxis']
                _date_of_death = args['date_of_death']
                _place_of_death = args['place_of_death']
                _reason_of_death = args['reason_of_death']
                _remarks = args['remarks']

                ret, status = create_veterinary_history(_lion_id, _microchip_number, _approx_age, _category_based_on_age, _sex,
                                              _if_female_lactating_or_not,_radio_collar_no,_frequency_on_reciever,_date_of_radio_collaring,
                                              _place_of_radio_collaring,_date_of_rescue,_place_of_rescue, _reason_of_rescue, _date_of_release, _place_of_release, _captivity_details, _clinical_observations, _veterinary_treatment_or_prophylaxis, _date_of_death, _place_of_death, _reason_of_death, _remarks)
                rv = dict()
                rv['status'] = status
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    #veterinary_history_details

    veterinary_history_info_parser = reqparse.RequestParser()
    veterinary_history_info_parser.add_argument('microchip_number',
                                      type=str,
                                      help='microchip_number of particular lion',
                                      required=True)
    veterinary_history_info_parser.add_argument('id',
                                                type=int,
                                                help='unique id of table',
                                                required=False)


    @api.route('/veterinary_history_info')
    @api.expect(veterinary_history_info_parser)
    class GetUserInfoService(Resource):
        @api.expect(veterinary_history_info_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = veterinary_history_info_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                microchip_number = args['microchip_number']
                id=args['id']
                rv, ret = veterinary_health_details(microchip_number, id)

                if ret != 0:
                    return rv, 404
                else:
                    return rv, 200
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
    
    health_check_parser = reqparse.RequestParser()
    health_check_parser.add_argument('var',
                                     type=int,
                                     help='dummy variable',
                                     required=True)

    @api.route('/health_check')
    @api.expect(health_check_parser)
    class HealthCheckService(Resource):
        @api.expect(health_check_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = health_check_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            rv = dict()
            rv['health'] = "good"
            return rv, 200

    edit_veterinary_history_parser = reqparse.RequestParser()
    #create_veterinary_history_parser = reqparse.RequestParser()
    edit_veterinary_history_parser.add_argument('id',
                                                type=int,
                                                help='id',
                                                required=True)

    edit_veterinary_history_parser.add_argument('lion_id',
                                                  type=str,
                                                  help='Lion_id',
                                                  required=True)
    edit_veterinary_history_parser.add_argument('microchip_number',
                                                  type=str,
                                                  help='microchip_number',
                                                  required=True)
    edit_veterinary_history_parser.add_argument('approx_age',
                                                  type=str,
                                                  help='approx_age',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('category_based_on_age',
                                                  type=str,
                                                  help='category_based_on_age',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('sex',
                                                  type=str,
                                                  help='sex',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('if_female_lactating_or_not',
                                                  type=str,
                                                  help='if_female_lactating_or_not',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('radio_collar_no',
                                                  type=str,
                                                  help='radio_collar_no',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('frequency_on_reciever',
                                                  type=str,
                                                  help='frequency_on_reciever',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('date_of_radio_collaring',
                                                  type=str,
                                                  help='date_of_radio_collaring',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('place_of_radio_collaring',
                                                  type=str,
                                                  help='place_of_radio_collaring',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('date_of_rescue',
                                                  type=str,
                                                  help='date_of_rescue',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('place_of_rescue',
                                                  type=str,
                                                  help='place_of_rescue',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('reason_of_rescue',
                                                  type=str,
                                                  help='reason_of_rescue',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('date_of_release',
                                                  type=str,
                                                  help='date_of_release',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('place_of_release',
                                                  type=str,
                                                  help='place_of_release',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('captivity_details',
                                                  type=str,
                                                  help='captivity_details',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('clinical_observations',
                                                  type=str,
                                                  help='clinical_observations',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('veterinary_treatment_or_prophylaxis',
                                                  type=str,
                                                  help='veterinary_treatment_or_prophylaxis',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('date_of_death',
                                                  type=str,
                                                  help='date_of_death',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('place_of_death',
                                                  type=str,
                                                  help='place_of_death',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('reason_of_death',
                                                  type=str,
                                                  help='reason_of_death',
                                                  required=False)
    edit_veterinary_history_parser.add_argument('remarks',
                                                  type=str,
                                                  help='remarks',
                                                  required=False)
    @api.route('/edit_veterinary_history')
    @api.expect(edit_veterinary_history_parser)
    class EditVeterinaryHistoryService(Resource):
        @api.expect(edit_veterinary_history_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = edit_veterinary_history_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            try:
                _id = args['id']
                _lion_id = args['lion_id']
                _microchip_number = args['microchip_number']
                _approx_age = args['approx_age']
                _category_based_on_age = args['category_based_on_age']
                _sex = args['sex']
                _if_female_lactating_or_not = args['if_female_lactating_or_not']
                _radio_collar_no = args['radio_collar_no']
                _frequency_on_reciever = args['frequency_on_reciever']
                _date_of_radio_collaring = args['date_of_radio_collaring']
                _place_of_radio_collaring = args['place_of_radio_collaring']
                _date_of_rescue = args['date_of_rescue']
                _place_of_rescue = args['place_of_rescue']
                _reason_of_rescue = args['reason_of_rescue']
                _date_of_release = args['date_of_release']
                _place_of_release = args['place_of_release']
                _captivity_details = args['captivity_details']
                _clinical_observations = args['clinical_observations']
                _veterinary_treatment_or_prophylaxis = args['veterinary_treatment_or_prophylaxis']
                _date_of_death = args['date_of_death']
                _place_of_death = args['place_of_death']
                _reason_of_death = args['reason_of_death']
                _remarks = args['remarks']
                ret_str, ret = update_veterinary_history(_id,_lion_id, _microchip_number, _approx_age, _category_based_on_age, _sex,
                                              _if_female_lactating_or_not,_radio_collar_no,_frequency_on_reciever,_date_of_radio_collaring,
                                              _place_of_radio_collaring,_date_of_rescue,_place_of_rescue, _reason_of_rescue, _date_of_release, _place_of_release, _captivity_details, _clinical_observations, _veterinary_treatment_or_prophylaxis, _date_of_death, _place_of_death, _reason_of_death, _remarks)
                rv = dict()
                rv['status'] = ret_str
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    #generate_report\
    # generate_report()
    # generate_report_parser = reqparse.RequestParser()
    # generate_report_parser.add_argument('microchip_number ',
    #                                   type=int,
    #                                   help='if particular lion pass chip number else leave blank',
    #                                   required=False)
    # @api.route('/generate_report')
    # @api.expect(generate_report_parser)
    # class GetLionPageService(Resource):
    #     @api.expect(generate_report_parser)
    #     @api.doc(responses={"response": 'json'})
    #     def post(self):
    #         try:
    #             args = generate_report_parser.parse_args()
    #         except Exception as e:
    #             rv = dict()
    #             rv['status'] = str(e)
    #             return rv, 404
    #         try:
    #             microchip_number = args['microchip_number']
    #             if microchip_number:
    #                 rv, ret = generate_report(microchip_number)
    #             else:
    #                 rv, ret = generate_report(microchip_number)
    #             if ret == 0:
    #                 return rv, 200
    #             else:
    #                 return rv, 404
    #         except Exception as e:
    #             rv = dict()
    #             rv['status'] = str(e)
    #             return rv, 404
    @api.route('/generate_report')
    class GetAllLionsService(Resource):
        @api.doc(responses={"response": "application/json"})
        def get(self):
            try:
                # ret, r = get_all_lions()
                # print("function call started")
                rv, ret =generate_report()

                if ret != 0:
                    return rv, 404
                else:
                    return rv, 200
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    delete_veterinary_history_parser = reqparse.RequestParser()
    delete_veterinary_history_parser.add_argument('id',
                                       type=str,
                                       help='The instance of lion id to be deleted',
                                       required=True)

    @api.route('/delete_veterinary_history')
    @api.expect(delete_veterinary_history_parser)
    class DeleteLionIDService(Resource):
        @api.expect(delete_veterinary_history_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = delete_veterinary_history_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            try:

                id = args['id']
                ret_str, ret = delete_veterinary_history(id)
                rv = dict()
                rv['status'] = ret_str
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
    return app




if __name__ == "__main__":
    serve(create_app(), host='0.0.0.0', port=8000, threads=20)

