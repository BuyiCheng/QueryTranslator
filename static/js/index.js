$(function(){
    $(".result").hide()
})

$(".btn-translate").on('click',function(){
    translation_type = $("select.languages option:selected").text()
    sql = $("#SQL").val()
    // alert(sql)
    // alert(translation_type)
    $.ajax({
        async: true,
        type: 'POST',
        url: '/translate',
        dataType:"json",
        data: {'type':translation_type, 'sql': sql},
        success:function(result){
            $(".result").show()
            $("#result-title").text(translation_type + ' Result')
            $("#tl-result").text(result.tl)
            console.log(result.tl)
        },
        error: function(error){
            alert('error:'+error)
        }
    })
});

$(".close").on('click', function(){
    $(".result").hide()
})